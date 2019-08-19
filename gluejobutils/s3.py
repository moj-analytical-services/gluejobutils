import json
import boto3
import botocore

from gluejobutils.utils import add_slash, remove_slash

try:
    # python 2.7
    from StringIO import StringIO
except:
    # python 3.6
    from io import StringIO

s3_resource = boto3.resource("s3")


def bucket_key_to_s3_path(bucket, key):
    """
    Takes an S3 bucket and key combination and returns the full S3 path to that location.
    """
    return "s3://{}/{}".format(bucket, key)


def s3_path_to_bucket_key(s3_path):
    """
    Splits out s3 file path to bucket key combination
    """
    s3_path = s3_path.replace("s3://", "")
    bucket, key = s3_path.split("/", 1)
    return bucket, key


def read_json_from_s3(s3_path, encoding="utf-8"):
    """
    read a json file from an s3 path
    """
    bucket, key = s3_path_to_bucket_key(s3_path)
    obj = s3_resource.Object(bucket, key)
    text = obj.get()["Body"].read().decode(encoding)
    return json.loads(text)


def write_json_to_s3(data, s3_path, indent=4, separators=(",", ": ")):
    """
    Saves your data to a json file (in memory and then sends it to the s3 path provided)
    """
    bucket, key = s3_path_to_bucket_key(s3_path)
    log_file = StringIO()
    json.dump(data, log_file, indent=indent, separators=separators)
    log_obj = s3_resource.Object(bucket, key)
    log_upload_resp = log_obj.put(Body=log_file.getvalue())
    return log_upload_resp


def get_filepaths_from_s3_folder(
    s3_folder_path, extension=None, exclude_zero_byte_files=True
):
    """
    Get a list of filepaths from a bucket. If extension is set to a string then only return files with that extension otherwise if set to None (default) all filepaths are returned.
    """
    if extension is None:
        extension = ""
    elif extension[0] != ".":
        extension = "." + extension

    s3_folder_path = add_slash(s3_folder_path)
    bucket, key = s3_path_to_bucket_key(s3_folder_path)

    s3b = s3_resource.Bucket(bucket)
    obs = s3b.objects.filter(Prefix=key)
    if exclude_zero_byte_files:
        ob_keys = [o.key for o in obs if o.key.endswith(extension) and o.size != 0]
    else:
        ob_keys = [o.key for o in obs if o.key.endswith(extension)]
    paths = sorted([bucket_key_to_s3_path(bucket, o) for o in ob_keys])

    return paths


def copy_s3_folder_contents_to_new_folder(old_s3_folder_path, new_s3_folder_path):
    """
    Copies complete folder structure within old_s3_folder_path to the new_s3_folder_path 
    """
    old_s3_folder_path = add_slash(old_s3_folder_path)
    new_s3_folder_path = add_slash(new_s3_folder_path)

    all_old_filepaths = get_filepaths_from_s3_folder(old_s3_folder_path)
    for ofp in all_old_filepaths:
        nfp = ofp.replace(old_s3_folder_path, new_s3_folder_path)
        copy_s3_object(ofp, nfp)


def delete_s3_object(s3_path):
    """
    Deletes the file at the s3_path given.
    """
    b, o = s3_path_to_bucket_key(s3_path)
    s3_resource.Object(b, o).delete()


def delete_s3_folder_contents(s3_folder_path):
    """
    Deletes all files within the s3_folder_path given given.
    """
    s3_folder_path = add_slash(s3_folder_path)
    all_filepaths = get_filepaths_from_s3_folder(
        s3_folder_path, exclude_zero_byte_files=False
    )
    for f in all_filepaths:
        delete_s3_object(f)


def copy_s3_object(old_s3_path, new_s3_path):
    """
    Copies a file in S3 from one location to another.
    """
    new_bucket, new_key = s3_path_to_bucket_key(new_s3_path)
    if "s3://" in old_s3_path:
        old_s3_path = old_s3_path.replace("s3://", "")
    s3_resource.Object(new_bucket, new_key).copy_from(CopySource=old_s3_path)


# https://stackoverflow.com/questions/33842944/check-if-a-key-exists-in-a-bucket-in-s3-using-boto3
def check_for_s3_file(s3_path):
    """
    Returns True/False depending if a file exists in the S3 path provided.
    exists = check_for_s3_file(s3://my-bucket/file.txt)
    """
    bucket, key = s3_path_to_bucket_key(s3_path)
    try:
        s3_resource.Object(bucket, key).load()
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        else:
            raise
    else:
        # The object does exist.
        return True


def folder_contains_only_files_with_extension(s3_folder_path, extension):
    """
    Checks if a folder contains any data (ingores zero-byte files) can also filter by extension.
    # Check if if folder only contains parquet files
    exists = folder_contains_only_files_with_extension("s3://my-bucket/data/", '.parquet').
    # Or check if only csv files exists in data
    exists = folder_contains_only_files_with_extension("s3://my-bucket/data/", '.csv')
    """
    if extension[0] != ".":
        extension = "." + extension
    all_p = get_filepaths_from_s3_folder(s3_folder_path)
    all_p_extension = [a for a in all_p if a.endswith(extension)]

    return len(all_p_extension) == len(all_p) and len(all_p_extension) != 0
