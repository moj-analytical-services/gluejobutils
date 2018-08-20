from gluejobutils.datatypes import translate_metadata_type_to_type, get_customschema_from_metadata
from gluejobutils.utils import add_slash, remove_slash
import json
import boto3
import botocore

try :
    # python 2.7
    from StringIO import StringIO
except :
    # python 3.6
    from io import StringIO

s3_resource = boto3.resource('s3')

def bucket_key_to_s3_path(bucket, key) :
    """
    Takes an S3 bucket and key combination and returns the full S3 path to that location.
    """
    key = add_slash(key)
    return 's3://{}/{}'.format(bucket, key)

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


def get_filepaths_from_s3_folder(s3_folder_path, parquet = False) :
    """
    Get a list of filepaths from a bucket. If parquet is set to True then only return a distinct list of folder
    paths that contain files with a .parquet extension.
    """
    s3_folder_path = add_slash(s3_folder_path)
    bucket, key = s3_path_to_bucket_key(s3_folder_path)

    s3b = s3_resource.Bucket(bucket)
    obs = s3b.objects.filter(Prefix = key)
    ob_keys = [o.key for o in obs]
    
    if parquet :
        ob_keys = list(set(["/".join(o.split('/')[:-1]) + '/' for o in ob_keys if o[-8:] == '.parquet']))

    ob_keys = sorted([bucket_key_to_s3_path(bucket, o) for o in ob_keys])

    return ob_keys

def copy_s3_folder_contents_to_new_folder(old_s3_folder_path, new_s3_folder_path) :
    old_s3_folder_path = add_slash(old_s3_folder_path)
    new_s3_folder_path = add_slash(new_s3_folder_path)

    old_bucket, old_obj_key = s3_path_to_bucket_key(old_s3_folder_path)
    all_old_filepaths = get_filepaths_from_s3_folder(old_bucket, old_obj_key)
    for ofp in all_old_filepaths :
        nfp = ofp.replace(old_s3_folder_path, new_s3_folder_path)
        copy_s3_object(ofp, nfp)

def delete_s3_folder_contents(s3_folder_path) :
    bucket, folder_path = s3_path_to_bucket_key(s3_folder_path)
    folder_path = add_slash(folder_path)
    all_filepaths = get_filepaths_from_s3_folder(bucket, folder_path)
    for f in all_filepaths :
        b, o = s3_path_to_bucket_key(f)
        s3_resource.Object(b, o).delete()

def copy_s3_object(old_s3_path, new_s3_path) :
    """
    Copies a file in S3 from one location to another.
    """
    new_bucket, new_key = s3_path_to_bucket_key(old_s3_path)
    s3_resource.Object(new_bucket, new_key).copy_from(CopySource=old_s3_path)

# https://stackoverflow.com/questions/33842944/check-if-a-key-exists-in-a-bucket-in-s3-using-boto3
def check_for_s3_file(s3_path) :
    """
    Returns True/False depending if a file exists in the S3 path provided.
    exists = check_for_s3_file(s3://my-bucket/file.txt)
    """
    bucket, key = s3_path_to_bucket_key(s3_path)
    try:
        s3_resource.Object(bucket, key).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        else:
            raise
    else:
        # The object does exist.
        return True

def check_for_parquet_data_in_folder(s3_folder_path) :
    """
    Checks if a folder contains parquet data. Used to test if parquet data is in s3 folder.
    exists = check_for_parquet_data_in_folder("s3://my-bucket/data/")
    """
    s3_folder_path = add_slash(s3_folder_path)
    return len(get_filepaths_from_s3_folder(s3_folder_path, parquet = True)) == 1
