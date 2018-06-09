from gluejobutils.datatypes import translate_metadata_type_to_type, get_customschema_from_metadata
import json
import boto3
s3_resource = boto3.resource('s3')

def s3_path_to_bucket_key(path):
    path = path.replace("s3://", "")
    bucket, key = path.split('/', 1)
    return bucket, key

def spark_read_csv_using_metadata_path(spark, metadata_path, csv_path, **kwargs):
    meta_json = read_json_from_s3(metadata_path)
    schema = get_customschema_from_metadata(meta_json)
    df = spark.read.csv(csv_path, schema=schema, **kwargs)
    return df

def read_json_from_s3(s3_path):
    bucket, key = s3_path_to_bucket_key(s3_path)
    obj = s3_resource.Object(bucket, key)
    text = obj.get()['Body'].read().decode('utf-8')
    return json.loads(text)