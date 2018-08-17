from gluejobutils.datatypes import translate_metadata_type_to_type, get_customschema_from_metadata
from gluejobutils.utils import add_slash, remove_slash
import json
import boto3

try :
    # python 2.7
    from StringIO import StringIO
except :
    # python 3.6
    from io import StringIO

s3_resource = boto3.resource('s3')

def s3_path_to_bucket_key(s3_path):
    """
    Splits out s3 file path to bucket key combination
    """
    s3_path = s3_path.replace("s3://", "")
    bucket, key = s3_path.split('/', 1)
    return bucket, key

def spark_read_csv_using_metadata_path(spark, metadata_path, csv_path, **kwargs):
    meta_json = read_json_from_s3(metadata_path)
    schema = get_customschema_from_metadata(meta_json)
    df = spark.read.csv(csv_path, schema=schema, **kwargs)
    return df

def read_json_from_s3(s3_path, encoding = 'utf-8') :
    """
    read a json file from an s3 path
    """
    bucket, key = s3_path_to_bucket_key(s3_path)
    obj = s3_resource.Object(bucket, key)
    text = obj.get()['Body'].read().decode(encoding)
    return json.loads(text)

def write_json_to_s3(data, s3_path) :
    """
    Saves your data to a json file (in memory and then sends it to the s3 path provided)
    """
    bucket, key = s3_path_to_bucket_key(s3_path)
    log_file = StringIO()
    json.dump(data, log_file, indent=4, separators=(',', ': '))
    log_obj = s3_resource.Object(bucket, key)
    log_upload_resp = log_obj.put(Body=log_file.getvalue())
    return log_upload_resp


def get_filepaths_from_s3_folder(s3_path, parquet = False) :
    """
    Get a list of filepaths from a bucket. If parquet is set to True then only return a distinct list of folder
    paths that contain files with a .parquet extension.
    """

    s3_path = add_slash(s3_path)
    bucket, key = s3_path_to_bucket_key(s3_path)

    s3b = s3_resource.Bucket(bucket)
    obs = s3b.objects.filter(Prefix = key)
    ob_keys = [o.key for o in obs]
    
    if parquet :
        ob_keys = list(set(["/".join(o.split('/')[:-1]) + '/' for o in ob_keys if o[-8:] == '.parquet']))

    ob_keys = sorted([create_s3_path(bucket, o) for o in ob_keys])

    return ob_keys

def copy_s3_folder_contents_to_new_folder(old_folder_path, new_folder_path) :
    old_folder_path = add_slash(old_folder_path)
    new_folder_path = add_slash(new_folder_path)

    old_bucket, old_obj_key = s3_path_to_bucket_key(old_folder_path)
    all_old_filepaths = get_filepaths_from_s3_folder(old_bucket, old_obj_key)
    for ofp in all_old_filepaths :
        nfp = ofp.replace(old_folder_path, new_folder_path)
        copy_s3_object(ofp, nfp)

# STOPPED HERE

def delete_s3_folder_contents(bucket, folder_path) :
    all_obs = get_filepaths_from_s3_bucket(bucket, folder_path)
    for o in all_obs :
        s3_resource.Object(bucket, o).delete()

def copy_s3_object(old_s3_path, new_s3_path) :
    new_bucket, new_key = s3_path_to_bucket_key(old_s3_path)
    s3_resource.Object(new_bucket, new_key).copy_from(CopySource=old_s3_path)

def create_s3_path(bucket, object_path) :
    object_path = add_slash(object_path)
    return 's3://{}/{}'.format(bucket, object_path)

def check_for_s3_file(bucket, file_key) :
    try:
        s3_resource.Object(bucket, file_key).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        else:
            raise ValueError("Unknown Error: boto did not return expected error on checking if file exists")
    else:
        # The object does exist.
        return True

def check_for_parquet_file(bucket, prefix) :
    return len(get_filepaths_from_s3_bucket(bucket, prefix, parquet = True)) == 1
