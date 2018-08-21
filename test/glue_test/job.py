# UNIT TEST FOR GLUEJOBUTILS
import sys
import os

# from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from gluejobutils import s3, utils

from datetime import datetime, timedelta

from StringIO import StringIO

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'metadata_base_path'])

sc = SparkContext()

glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

log_file = StringIO()

## =====================> UTILS MODULE TESTING <========================= ##
a = 'test/folder/path/'
b = 'test/folder/path'
if utils.add_slash(a) != a :
    raise ValueError('add_slash FAILURE')
if utils.remove_slash(a) != b :
    raise ValueError('remove_slash FAILURE')
if utils.add_slash(b) != a :
    raise ValueError('add_slash FAILURE')
if utils.remove_slash(b) != b :
    raise ValueError('remove_slash FAILURE')

## =====================> S3 MODULE TESTING <========================= ##
bucket = 'alpha-gluejobutils'
diamonds_obj = 'testing/data/diamonds.csv'
### ### ### ### ### ### ### ### 
### bucket_key_to_s3_path ### 
### ### ### ### ### ### ### ###
out = s3.bucket_key_to_s3_path(bucket, diamonds_obj)
if out != 's3://alpha-gluejobutils/testing/data/diamonds.csv' :
    raise ValueError('bucket_key_to_s3_path FAILURE')

out = s3.bucket_key_to_s3_path(bucket, 'some/path/')
if out != 's3://alpha-gluejobutils/some/path/' :
    raise ValueError('bucket_key_to_s3_path FAILURE')
    
out = s3.bucket_key_to_s3_path(bucket, 'some/path')
if out != 's3://alpha-gluejobutils/some/path' :
    raise ValueError('bucket_key_to_s3_path FAILURE')

log_file.write("===> bucket_key_to_s3_path ===> OK\n")

### ### ### ### ### ### ### ### 
### s3_path_to_bucket_key ### 
### ### ### ### ### ### ### ### 
b, o = s3.s3_path_to_bucket_key('s3://alpha-gluejobutils/testing/data/diamonds_csv/diamonds.csv')
if b != 'alpha-gluejobutils' or o != 'testing/data/diamonds_csv/diamonds.csv' :
    raise ValueError('s3_path_to_bucket_key FAILURE')

b, o = s3.s3_path_to_bucket_key('s3://alpha-gluejobutils/testing/data')
if b != 'alpha-gluejobutils' or o != 'testing/data' :
    raise ValueError('s3_path_to_bucket_key FAILURE')

b, o = s3.s3_path_to_bucket_key('s3://alpha-gluejobutils/testing/data/')
if b != 'alpha-gluejobutils' or o != 'testing/data/' :
    raise ValueError('s3_path_to_bucket_key FAILURE')

print("===> s3_path_to_bucket_key ===> OK")

### ### ### ### ### ### ###
### read_json_from_s3 ### 
### ### ### ### ### ### ### 
test_json = s3.read_json_from_s3('s3://alpha-gluejobutils/testing/meta_data/diamonds.json')
diff = len(set(['$schema', 'name', 'description', 'data_format', 'columns', 'partitions', 'location']).difference(test_json.keys()))
if diff != 0 :
    raise ValueError('read_json_from_s3 FAILURE')
print("===> read_json_from_s3 ===> OK")

### ### ### ### ### ###
### write_json_to_s3 ### 
### ### ### ### ### ###
json_data = {'a':'dog', 'b':14,'c':[1,2,3],'d':{'cat': 'alpha'}}
s3.write_json_to_s3(json_data, 's3://alpha-gluejobutils/testing/data_dump/test1.json')
json_data2 = s3.read_json_from_s3('s3://alpha-gluejobutils/testing/data_dump/test1.json')

if json_data != json_data2 :
    raise ValueError('read_json_from_s3 FAILURE')
print("===> write_json_to_s3 ===> OK")

### ### ### ### ### ### ###  
### check_for_s3_file ###
### ### ### ### ### ### ### 
if not s3.check_for_s3_file('s3://alpha-gluejobutils/testing/meta_data/diamonds.json') :
    raise ValueError('check_for_s3_file FAILURE')
print("===> check_for_s3_file ===> OK")

### ### ### ### ### ###### ### ### ### 
### get_filepaths_from_s3_folder ### 
### ### ### ### ### ### ### ### ### ### 
out1 = s3.get_filepaths_from_s3_folder('s3://alpha-gluejobutils/testing')
out2 = s3.get_filepaths_from_s3_folder('s3://alpha-gluejobutils/testing/')
for o1, o2 in zip(out1, out2) :
    if o1 != o2 :
        raise ValueError('get_filepaths_from_s3_folder FAILURE')
        
out3 = s3.get_filepaths_from_s3_folder('s3://alpha-gluejobutils/testing/data/diamonds_parquet/', parquet=True)
if out3[0] != 's3://alpha-gluejobutils/testing/data/diamonds_parquet/' :
    raise ValueError('get_filepaths_from_s3_folder FAILURE')
if len(out3) != 1 :
    raise ValueError('get_filepaths_from_s3_folder FAILURE')

out4 = s3.get_filepaths_from_s3_folder('s3://alpha-gluejobutils/testing/data/diamonds_parquet', parquet=True)
if out4[0] != 's3://alpha-gluejobutils/testing/data/diamonds_parquet/' :
    raise ValueError('get_filepaths_from_s3_folder FAILURE')
if len(out4) != 1 :
    raise ValueError('get_filepaths_from_s3_folder FAILURE')
print("===> get_filepaths_from_s3_folder ===> OK")

### ### ### ### ### ###
### copy_s3_object ###
### ### ### ### ### ###
s3.copy_s3_object(old_s3_path='s3://alpha-gluejobutils/testing/meta_data/diamonds.json', new_s3_path='s3://alpha-gluejobutils/testing/data_dump/copy_test/file0.json')
s3.copy_s3_object(old_s3_path='s3://alpha-gluejobutils/testing/meta_data/diamonds.json', new_s3_path='s3://alpha-gluejobutils/testing/data_dump/copy_test/file1.json')
json0 = s3.read_json_from_s3('s3://alpha-gluejobutils/testing/data_dump/copy_test/file0.json')
json1 = s3.read_json_from_s3('s3://alpha-gluejobutils/testing/data_dump/copy_test/file1.json')
if json0 != json1 :
    raise ValueError('copy_s3_object FAILURE')
    
print("===> copy_s3_object ===> OK")

### ### ### ### ### ### ### ### ### ### ### ### 
### copy_s3_folder_contents_to_new_folder ### 
### ### ### ### ### ### ### ### ### ### ### ### 
for i in [1,2,3] :
    s3.copy_s3_object(old_s3_path='s3://alpha-gluejobutils/testing/data_dump/copy_test/file0.json', new_s3_path='s3://alpha-gluejobutils/testing/data_dump/copy_test/file{}.json'.format(i))

s3.copy_s3_folder_contents_to_new_folder('s3://alpha-gluejobutils/testing/data_dump/copy_test/', 's3://alpha-gluejobutils/testing/data_dump/copy_test2/')
for i, p in enumerate(s3.get_filepaths_from_s3_folder('s3://alpha-gluejobutils/testing/data_dump/copy_test2/')) :
    if p.split('/')[-1] != 'file{}.json'.format(i) :
        raise ValueError('copy_s3_folder_contents_to_new_folder FAILURE')
print("===> copy_s3_folder_contents_to_new_folder ===> OK")


### ### ### ### ### ### ###
### delete_s3_object ###
### ### ### ### ### ### ###
s3.copy_s3_object('s3://alpha-gluejobutils/testing/data_dump/copy_test/file0.json', 's3://alpha-gluejobutils/testing/data_dump/copy_test/copy_file0.json')

s3.delete_s3_object('s3://alpha-gluejobutils/testing/data_dump/copy_test/copy_file0.json')

if s3.check_for_s3_file('s3://alpha-gluejobutils/testing/data_dump/copy_test/copy_file0.json') :
    raise ValueError('delete_s3_object FAILURE')
print("===> delete_s3_object ===> OK")


### ### ### ### ### ### ### ### ###
### delete_s3_folder_contents ###
### ### ### ### ### ### ### ### ###
s3.copy_s3_folder_contents_to_new_folder('s3://alpha-gluejobutils/testing/data_dump/copy_test/', 's3://alpha-gluejobutils/testing/data_dump/copy_test2/')
s3.delete_s3_folder_contents('s3://alpha-gluejobutils/testing/data_dump/copy_test2/')
if len(s3.get_filepaths_from_s3_folder('s3://alpha-gluejobutils/testing/data_dump/copy_test2/')) != 0 :
    raise ValueError('get_filepaths_from_s3_folder FAILURE')
    
s3.copy_s3_folder_contents_to_new_folder('s3://alpha-gluejobutils/testing/data_dump/copy_test', 's3://alpha-gluejobutils/testing/data_dump/copy_test2')
s3.delete_s3_folder_contents('s3://alpha-gluejobutils/testing/data_dump/copy_test2')
if len(s3.get_filepaths_from_s3_folder('s3://alpha-gluejobutils/testing/data_dump/copy_test2')) != 0 :
    raise ValueError('get_filepaths_from_s3_folder FAILURE')
print("===> delete_s3_folder_contents ===> OK")


### ### ### ### ### ### ### ### ### ### ###
### check_for_parquet_data_in_folder ###
### ### ### ### ### ### ### ### ### ### ###
if s3.check_for_parquet_data_in_folder('s3://alpha-gluejobutils/testing/data/diamonds_parquet/') != True :
    raise ValueError('check_for_parquet_data_in_folder FAILURE')
if s3.check_for_parquet_data_in_folder('s3://alpha-gluejobutils/testing/data/diamonds_parquet') != True :
    raise ValueError('check_for_parquet_data_in_folder FAILURE')
if s3.check_for_parquet_data_in_folder('s3://alpha-gluejobutils/testing/data/diamonds_csv') != False :
    raise ValueError('check_for_parquet_data_in_folder FAILURE')
print("===> check_for_parquet_data_in_folder ===> OK")     

job.commit()
