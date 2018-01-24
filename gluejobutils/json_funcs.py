import json
import boto3

from gluejobutils.s3 import s3_path_to_bucket_key
s3_resource = boto3.resource('s3')

def read_json_from_s3(s3_path):
    bucket, key = s3_path_to_bucket_key(s3_path)
    obj = s3_resource.Object(bucket, key)
    text = obj.get()['Body'].read().decode('utf-8')
    return json.loads(text)

def read_json(filename) :
    with open(filename) as json_data:
        data = json.load(json_data)
    return data

def write_json(data, filename) :
    with open(filename, 'w') as outfile:
        json.dump(data, outfile, indent=4, separators=(',', ': '))

