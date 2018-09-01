# UNIT TEST FOR GLUEJOBUTILS
import sys
import os

# from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from gluejobutils import s3, utils, dea_record_datetimes as drd, datatypes, update_table_operations as uto

import datetime
from pyspark.sql import Row, functions as F
from pyspark.sql.types import IntegerType

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'metadata_base_path'])

sc = SparkContext()

glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print(sys.version)


### ### ### ### ### ### ### ### 
### dea_record_start_date ###
### ### ### ### ### ### ### ### 
csv_path = 's3://alpha-gluejobutils/testing/data/diamonds_csv/'
meta_path = 's3://alpha-gluejobutils/testing/meta_data/diamonds.json'
start_date = '2018-01-01'
start_datetime = '2018-01-01 01:00:00'
meta = datatypes.create_spark_schema_from_metadata_file(meta_path)
df = spark.read.csv(csv_path, header = True, schema = meta)
df = drd.set_dea_record_start_datetime(df, '2018-01-01 01:00:00')
df.write.partitionBy('dea_record_start_datetime').mode('overwrite').format('parquet').save('s3://alpha-gluejobutils/silly_folder4/')

## =====================> INIT TEST MODULE TESTING <========================= ##
s3.delete_s3_folder_contents('s3://alpha-gluejobutils/testing/data_dump/')
s3.delete_s3_folder_contents('s3://alpha-gluejobutils/database/')

csv_path = 's3://alpha-gluejobutils/testing/data/diamonds_csv/'
meta_path = 's3://alpha-gluejobutils/testing/meta_data/diamonds.json'

meta = datatypes.create_spark_schema_from_metadata_file(meta_path)
df_old = spark.read.csv(csv_path, header = True, schema=meta)
df_old = drd.init_dea_record_datetimes(df_old, '2018-01-01 01:00:00')
df_old.write.mode('overwrite').parquet('s3://alpha-gluejobutils/database/table1/')

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
print("===> utils ===> OK")


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

print("===> bucket_key_to_s3_path ===> OK")


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
        
out3 = s3.get_filepaths_from_s3_folder('s3://alpha-gluejobutils/testing/data/', extension='.parquet')
if 'diamonds_parquet' not in out3[0] :
    raise ValueError('get_filepaths_from_s3_folder FAILURE')
if len(out3) != 1 :
    raise ValueError('get_filepaths_from_s3_folder FAILURE')

out4 = s3.get_filepaths_from_s3_folder('s3://alpha-gluejobutils/testing/data/diamonds_parquet', extension='parquet')
if 'diamonds_parquet' not in out4[0] :
    raise ValueError('get_filepaths_from_s3_folder FAILURE')
if len(out4) != 1 :
    raise ValueError('get_filepaths_from_s3_folder FAILURE')
    
out5 = s3.get_filepaths_from_s3_folder('s3://alpha-gluejobutils/testing/data/')
if any([p.endswith('/') or '_SUCCESS' in p for p in out5]) :
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
### folder_contains_only_files_with_extension ###
### ### ### ### ### ### ### ### ### ### ###
s3_folder_path1 = 's3://alpha-gluejobutils/database/table1/nothing'
s3_folder_path2 = 's3://alpha-gluejobutils/database/table1/'
s3_folder_path3 = 's3://alpha-gluejobutils/testing/data/diamonds_csv/'
s3_folder_path4 = 's3://alpha-gluejobutils/testing/data/diamonds_parquet/'
s3_folder_path5 = 's3://alpha-gluejobutils/testing/data/'

if s3.folder_contains_only_files_with_extension(s3_folder_path1, '.parquet') != False:
    raise ValueError('folder_contains_only_files_with_extension FAILURE')
if s3.folder_contains_only_files_with_extension(s3_folder_path2, 'parquet') != True :
    raise ValueError('folder_contains_only_files_with_extension FAILURE')
if s3.folder_contains_only_files_with_extension(s3_folder_path3, 'csv') != True : 
    raise ValueError('folder_contains_only_files_with_extension FAILURE')
if s3.folder_contains_only_files_with_extension(s3_folder_path4, '.parquet') != True :
    raise ValueError('folder_contains_only_files_with_extension FAILURE')
if s3.folder_contains_only_files_with_extension(s3_folder_path5, 'parquet') != False :
    raise ValueError('folder_contains_only_files_with_extension FAILURE')

print("===> folder_contains_only_files_with_extension ===> OK")  


## =====================> DATATYPE MODULE TESTING <========================= ##

### ### ### ### ### ### ### ### ### ### ###
### translate_metadata_type_to_type ###
### ### ### ### ### ### ### ### ### ### ###
test_types = {
    "character" : {"glue" : "string", "spark": "StringType"},
    "int" : {"glue" : "int", "spark": "IntegerType"},
    "long" : {"glue" : "bigint", "spark": "LongType"},
    "float" : {"glue" : "float", "spark": "FloatType"},
    "double" : {"glue" : "double", "spark": "DoubleType"},
    "date" : {"glue" : "date", "spark": "DateType"},
    "datetime" : {"glue" : "timestamp", "spark": "TimestampType"},
    "boolean" : {"glue" : "boolean", "spark": "BooleanType"}
}
for k in test_types.keys() :
    if datatypes.translate_metadata_type_to_type(k, "glue") != test_types[k]["glue"] :
        raise ValueError("translate_metadata_type_to_type FAILURE")
    if datatypes.translate_metadata_type_to_type(k, "spark") != test_types[k]["spark"] :
        raise ValueError("translate_metadata_type_to_type FAILURE")

print("===> translate_metadata_type_to_type ===> OK")   


### ### ### ### ### ### ### ### ### ### ###
### create_spark_schema_from_metadata ###
### ### ### ### ### ### ### ### ### ### ###
metadata = s3.read_json_from_s3('s3://alpha-gluejobutils/testing/meta_data/diamonds.json')
schema = datatypes.create_spark_schema_from_metadata(metadata)
schema2 = datatypes.create_spark_schema_from_metadata_file('s3://alpha-gluejobutils/testing/meta_data/diamonds.json')
if schema != schema2 :
    raise ValueError("create_spark_schema_from_metadata | create_spark_schema_from_metadata_file FAILURE")
    
for s, m in zip(schema, metadata['columns']) :
    if s.name != m['name'] :
        raise ValueError('create_spark_schema_from_metadata FAILURE')
        
schema3 = datatypes.create_spark_schema_from_metadata(metadata, exclude_cols=['carat'], non_nullable_cols=['x','y','z'])        
if 'carat' in [s.name for s in schema3] :
    raise ValueError('create_spark_schema_from_metadata FAILURE')

    
non_nulls = [s.name for s in schema3 if not s.nullable]
if non_nulls != ['x','y','z'] :
    raise ValueError('create_spark_schema_from_metadata FAILURE')
print("===> create_spark_schema_from_metadata ===> OK")


### ### ### ### ### ### 
### align_df_to_meta ###
### ### ### ### ### ### 
from gluejobutils import s3
from gluejobutils import datatypes

csv_path = 's3://alpha-gluejobutils/testing/data/diamonds_csv/'
meta_path = 's3://alpha-gluejobutils/testing/meta_data/diamonds.json'
meta = s3.read_json_from_s3(meta_path)
df = spark.read.csv(csv_path, header = True)
df2 = datatypes.align_df_to_meta(df, meta)
if df2.count() != df.count() :
    raise ValueError('align_df_to_meta FAILURE') 
for dc, dc2 in zip(df.columns, df2.columns) :
    if dc != dc2 :
        raise ValueError('align_df_to_meta FAILURE')
for s, c in zip(df2.schema, meta['columns']) :
    if s.name != c['name'] :
        raise ValueError('align_df_to_meta FAILURE')
    if str(s.dataType) != datatypes.translate_metadata_type_to_type(c['type'], 'spark') :
        raise ValueError('align_df_to_meta FAILURE')

df3 = datatypes.align_df_to_meta(df, meta, exclude_columns=['x','y','z'])
if any([e in df3.columns for e in ['x','y','z']]) :
    raise ValueError('align_df_to_meta FAILURE')

meta['columns'].append({"name" : "date_test", "type" : 'date', 'description' : ''})
meta['columns'].append({"name" : "datetime_test", "type" : 'datetime', 'description' : ''})

df3 = df3.withColumn('date_test', F.to_date(F.lit("2018-01-01")))
df3 = df3.withColumn('datetime_test', F.to_timestamp(F.lit("2018-01-01 00:00:00")))
try :
    df4 = datatypes.align_df_to_meta(df3, meta)
except ValueError as e :
    if 'ETL_ERROR' not in str(e) :
        raise ValueError('align_df_to_meta FAILURE')
        
df4 = datatypes.align_df_to_meta(df3, meta, null_missing_cols=True)
fr = df4.take(1)[0].asDict()
if fr["diamond_id"] != 0 or fr["x"] is not None or fr["y"] is not None or fr["z"] is not None :
    raise ValueError('align_df_to_meta FAILURE')
print("===> align_df_to_meta ===> OK") 


## =====================> DEA_RECORD_DATETIMES MODULE TESTING <========================= ##

### ### ### ### ### ### ### ### 
###python and java timestamp ###
### ### ### ### ### ### ### ### 
if datetime.datetime.strptime(drd.static_record_end_datetime, drd.standard_datetime_format_python).strftime(drd.standard_datetime_format_python) != drd.static_record_end_datetime :
    raise ValueError("python and java timestamp formats do not match")
print("===> python to java timestamp convertion ===> OK")


### ### ### ### ### ### ### ### 
### dea_record_start_date ###
### ### ### ### ### ### ### ### 
csv_path = 's3://alpha-gluejobutils/testing/data/diamonds_csv/'
meta_path = 's3://alpha-gluejobutils/testing/meta_data/diamonds.json'
start_date = '2018-01-01'
start_datetime = '2018-01-01 01:00:00'
meta = datatypes.create_spark_schema_from_metadata_file(meta_path)
df = spark.read.csv(csv_path, header = True, schema = meta)
df = drd.set_dea_record_start_datetime(df, '2018-01-01 01:00:00')

if df.take(1)[0].asDict()['dea_record_start_datetime'].strftime('%Y-%m-%d %H:%M:%S') != start_datetime :
    raise ValueError('set_dea_record_start_date FAILURE')
    
df = drd.set_dea_record_start_datetime(df, '12:34:56 31/12/2017', datetime_format="HH:mm:ss dd/MM/yyyy")
if df.take(1)[0].asDict()['dea_record_start_datetime'].strftime('%Y-%m-%d %H:%M:%S') != '2017-12-31 12:34:56' :
    raise ValueError('set_dea_record_start_datetime FAILURE')

if not isinstance(df.take(1)[0].asDict()['dea_record_start_datetime'], datetime.datetime) :
    raise ValueError('dea_record_start_date FAILURE')
print("===> set_dea_record_start_datetime ===> OK") 


### ### ### ### ### ### ### ### 
### set_dea_record_end_date ###
### ### ### ### ### ### ### ### 
df = drd.set_dea_record_end_datetime(df)
end_datetime = '2999-01-01 00:00:00'
if df.take(1)[0].asDict()['dea_record_end_datetime'].strftime('%Y-%m-%d %H:%M:%S') != end_datetime :
    raise ValueError('dea_record_end_date FAILURE')
if not isinstance(df.take(1)[0].asDict()['dea_record_end_datetime'], datetime.datetime) :
    raise ValueError('dea_record_end_date FAILURE')

df = drd.set_dea_record_end_datetime(df, '2018-01-01 01:00:00')
if df.take(1)[0].asDict()['dea_record_end_datetime'].strftime('%Y-%m-%d %H:%M:%S') != '2018-01-01 01:00:00' :
    raise ValueError('dea_record_end_date FAILURE')
print("===> set_dea_record_end_datetime ===> OK") 


### ### ### ### ### ### ### ### 
### init_dea_record_dates ###
### ### ### ### ### ### ### ### 
start_datetime = '2017-01-01 12:51:43'
end_datetime = '2999-01-01 00:00:00'

meta = datatypes.create_spark_schema_from_metadata_file(meta_path)
df = spark.read.csv(csv_path, header = True, schema=meta)
df = drd.init_dea_record_datetimes(df, start_datetime)

if df.take(1)[0].asDict()['dea_record_start_datetime'].strftime('%Y-%m-%d %H:%M:%S') != start_datetime :
    raise ValueError('init_dea_record_dates FAILURE')
if not isinstance(df.take(1)[0].asDict()['dea_record_start_datetime'], datetime.datetime) :
    raise ValueError('init_dea_record_dates FAILURE')
if df.take(1)[0].asDict()['dea_record_end_datetime'].strftime('%Y-%m-%d %H:%M:%S') != end_datetime :
    raise ValueError('init_dea_record_dates FAILURE')
if not isinstance(df.take(1)[0].asDict()['dea_record_end_datetime'], datetime.datetime) :
    raise ValueError('init_dea_record_dates FAILURE')
print("===> init_dea_record_datetimes ===> OK")


### ### ### ### ### ### ### ### ### ###
### update_dea_record_end_datetime ###
### ### ### ### ### ### ### ### ### ###
test1_ans = spark.createDataFrame([Row(dea_record_start_datetime=datetime.datetime(2018, 1, 1, 1, 0), dea_record_end_datetime=datetime.datetime(2018, 1, 1, 1, 23, 45)),
                                   Row(dea_record_start_datetime=datetime.datetime(2018, 1, 1, 1, 0), dea_record_end_datetime=datetime.datetime(2999, 1, 1, 0, 0)),
                                   Row(dea_record_start_datetime=datetime.datetime(2018, 1, 1, 1, 23, 45), dea_record_end_datetime=datetime.datetime(2999, 1, 1, 0, 0))]).select('dea_record_start_datetime', 'dea_record_end_datetime')

test2_ans = spark.createDataFrame([Row(dea_record_start_datetime=datetime.datetime(2018, 1, 1, 1, 0), dea_record_end_datetime=datetime.datetime(2018, 1, 1, 1, 23, 45)),
                                   Row(dea_record_start_datetime=datetime.datetime(2018, 1, 1, 1, 23, 45), dea_record_end_datetime=datetime.datetime(2999, 1, 1, 0, 0))]).select('dea_record_start_datetime', 'dea_record_end_datetime')

test3_ans = spark.createDataFrame([Row(dea_record_start_datetime=datetime.datetime(2018, 1, 1, 1, 0), dea_record_end_datetime=datetime.datetime(2999, 1, 1, 0, 0))]).select('dea_record_start_datetime', 'dea_record_end_datetime')

meta = datatypes.create_spark_schema_from_metadata_file(meta_path)
df_old = spark.read.csv(csv_path, header = True, schema=meta)
df_new = spark.read.csv(csv_path, header = True).filter('diamond_id < 1000')

df_old = drd.init_dea_record_datetimes(df_old, '2018-01-01 01:00:00')
df_new = drd.init_dea_record_datetimes(df_new, '2018-01-01 01:23:45')

df = df_old.union(df_new)
df = drd.update_dea_record_end_datetime(df, 'diamond_id')

if df.count() != 54940 :
    raise ValueError('update_dea_record_end_date FAILURE')
    
df.createOrReplaceTempView('df')
test1 = spark.sql("SELECT DISTINCT dea_record_start_datetime, dea_record_end_datetime FROM df ORDER BY dea_record_start_datetime, dea_record_end_datetime")
test2 = spark.sql("SELECT DISTINCT dea_record_start_datetime, dea_record_end_datetime FROM df WHERE diamond_id < 1000 ORDER BY dea_record_start_datetime, dea_record_end_datetime")
test3 = spark.sql("SELECT DISTINCT dea_record_start_datetime, dea_record_end_datetime FROM df WHERE diamond_id >= 1000 ORDER BY dea_record_start_datetime, dea_record_end_datetime")

if sorted(test1.collect()) != sorted(test1_ans.collect()) :
    raise ValueError("update_dea_record_end_date FAILURE")
if sorted(test2.collect()) != sorted(test2_ans.collect()) :
    raise ValueError("update_dea_record_end_date FAILURE")
if sorted(test3.collect()) != sorted(test3_ans.collect()) :
    raise ValueError("update_dea_record_end_date FAILURE")
print("===> update_dea_record_end_datetime ===> OK")


### ### ### ### ### ### ### ### ### ### ### ### ###
### insert_df_into_table_and_update_scd_by_row ###
### ### ### ### ### ### ### ### ### ### ### ### ###
bucket = 'alpha-gluejobutils'

db_table_path = 's3://{}/database/table1/'.format(bucket)
db_table_path_tmp = 's3://{}/database/table1_tmp/'.format(bucket)

meta = datatypes.create_spark_schema_from_metadata_file(meta_path)
df_old = spark.read.csv(csv_path, header = True, schema=meta)
df_old = drd.init_dea_record_datetimes(df_old, '2018-01-01 01:00:00')
df_old.write.mode('overwrite').parquet(db_table_path)

df_new = spark.read.csv(csv_path, header = True, schema=meta).filter('diamond_id < 1000')
df_new = drd.init_dea_record_datetimes(df_new, '2018-01-01 01:23:45')

uto.insert_df_into_table_and_update_scd_by_row(spark=spark, new_df=df_new, table_db_base_path=db_table_path, update_by_cols = ['diamond_id'], coalesce_size = 4, update_latest_records_only = True)

if spark.read.parquet(db_table_path_tmp).count() != 54940 :
    raise ValueError('insert_df_into_table_and_update_scd_by_row FAILURE')
if spark.read.parquet(db_table_path_tmp+'dea_record_update_type=old/').count() != 52940 :
    raise ValueError('insert_df_into_table_and_update_scd_by_row FAILURE')
if spark.read.parquet(db_table_path_tmp+'dea_record_update_type=new/').count() != 2000 :
    raise ValueError('insert_df_into_table_and_update_scd_by_row FAILURE')

spark.read.parquet(db_table_path_tmp).createOrReplaceTempView('df')
test1 = spark.sql("SELECT DISTINCT dea_record_start_datetime, dea_record_end_datetime FROM df")
test2 = spark.sql("SELECT DISTINCT dea_record_start_datetime, dea_record_end_datetime FROM df where dea_record_update_type='new'")
test3 = spark.sql("SELECT DISTINCT dea_record_start_datetime, dea_record_end_datetime FROM df where dea_record_update_type='old'")

if sorted(test1.collect()) != sorted(test1_ans.collect()) :
    raise ValueError("update_dea_record_end_date FAILURE")
if sorted(test2.collect()) != sorted(test2_ans.collect()) :
    raise ValueError("update_dea_record_end_date FAILURE")
if sorted(test3.collect()) != sorted(test3_ans.collect()) :
    raise ValueError("update_dea_record_end_date FAILURE")
print("===> insert_df_into_table_and_update_scd_by_row () ===> OK")


### Test write_update_from_tmp_to_table_db (1/3) ###
uto.write_update_from_tmp_to_table_db(spark, db_table_path, partition_path = '', coalesce_size = 4)
if spark.read.parquet(db_table_path).count() != 54940 :
    raise ValueError('write_update_from_tmp_to_table_db FAILURE')

spark.read.parquet(db_table_path_tmp).createOrReplaceTempView('df')
test1 = spark.sql("SELECT DISTINCT dea_record_start_datetime, dea_record_end_datetime FROM df")
if sorted(test1.collect()) != sorted(test1_ans.collect()) :
    raise ValueError("write_update_from_tmp_to_table_db FAILURE")
print("===> write_update_from_tmp_to_table_db (1/3) ===> OK")


db_table_path = 's3://{}/database/table2/'.format(bucket)
db_table_path_tmp = 's3://{}/database/table2_tmp/'.format(bucket)

meta = datatypes.create_spark_schema_from_metadata_file(meta_path)
df_old = spark.read.csv(csv_path, header = True, schema=meta)
df_old = drd.init_dea_record_datetimes(df_old, '2018-01-01 01:00:00')
df_old = df_old.withColumn('partition_bin', F.floor(df_old.diamond_id / 10000).cast(IntegerType()))
df_old.write.mode('overwrite').partitionBy('partition_bin').parquet(db_table_path)

df_new = df_old.filter('diamond_id < 1000')
df_new = drd.init_dea_record_datetimes(df_new, '2018-01-01 01:23:45').drop('partition_bin')

uto.insert_df_into_table_and_update_scd_by_row(spark=spark, new_df=df_new, table_db_base_path=db_table_path, update_by_cols=['diamond_id'], partition_path='partition_bin=0')

if spark.read.parquet(db_table_path_tmp).count() != 11000 :
    raise ValueError('insert_df_into_table_and_update_scd_by_row FAILURE')
if spark.read.parquet(db_table_path_tmp + '/partition_bin=0/dea_record_update_type=old/').count() != 9000 :
    raise ValueError('insert_df_into_table_and_update_scd_by_row FAILURE')
if spark.read.parquet(db_table_path_tmp+'/partition_bin=0/dea_record_update_type=new/').count() != 2000 :
    raise ValueError('insert_df_into_table_and_update_scd_by_row FAILURE')

spark.read.parquet(db_table_path_tmp).createOrReplaceTempView('df')
test1 = spark.sql("SELECT DISTINCT dea_record_start_datetime, dea_record_end_datetime FROM df")
test2 = spark.sql("SELECT DISTINCT dea_record_start_datetime, dea_record_end_datetime FROM df where dea_record_update_type='new'")
test3 = spark.sql("SELECT DISTINCT dea_record_start_datetime, dea_record_end_datetime FROM df where dea_record_update_type='old'")

if sorted(test1.collect()) != sorted(test1_ans.collect()) :
    raise ValueError("update_dea_record_end_date FAILURE")
if sorted(test2.collect()) != sorted(test2_ans.collect()) :
    raise ValueError("update_dea_record_end_date FAILURE")
if sorted(test3.collect()) != sorted(test3_ans.collect()) :
    raise ValueError("update_dea_record_end_date FAILURE")
print("===> insert_df_into_table_and_update_scd_by_row (2/2) ===> OK")


### Test write_update_from_tmp_to_table_db (2/3) ###
uto.write_update_from_tmp_to_table_db(spark, db_table_path, partition_path = 'partition_bin=0', coalesce_size = 2)
if spark.read.parquet(db_table_path).count() != 54940 :
    raise ValueError('write_update_from_tmp_to_table_db FAILURE')

spark.read.parquet(db_table_path).createOrReplaceTempView('df')
test1 = spark.sql("SELECT DISTINCT dea_record_start_datetime, dea_record_end_datetime FROM df")
if sorted(test1.collect()) != sorted(test1_ans.collect()) :
    raise ValueError("write_update_from_tmp_to_table_db FAILURE")
print("===> write_update_from_tmp_to_table_db (2/3) ===> OK")


### ### ### ### ### ### ### ### ### ### ### ### ###
### insert_df_into_table_and_update_scd_by_row ###
### ### ### ### ### ### ### ### ### ### ### ### ###
test1_ans = spark.createDataFrame([Row(dea_record_start_datetime=datetime.datetime(2018, 1, 1, 1, 0), dea_record_end_datetime=datetime.datetime(2018, 1, 1, 2, 0)), 
                                   Row(dea_record_start_datetime=datetime.datetime(2018, 1, 1, 2, 0), dea_record_end_datetime=datetime.datetime(2999, 1, 1, 0, 0))]).select('dea_record_start_datetime', 'dea_record_end_datetime')

test2_ans = spark.createDataFrame([Row(dea_record_start_datetime=datetime.datetime(2018, 1, 1, 2, 0), dea_record_end_datetime=datetime.datetime(2999, 1, 1, 0, 0))]).select('dea_record_start_datetime', 'dea_record_end_datetime')

test3_ans = spark.createDataFrame([Row(dea_record_start_datetime=datetime.datetime(2018, 1, 1, 1, 0), dea_record_end_datetime=datetime.datetime(2018, 1, 1, 2, 0))]).select('dea_record_start_datetime', 'dea_record_end_datetime')

db_table_path = 's3://{}/database/table3/'.format(bucket)
db_table_path_tmp = 's3://{}/database/table3_tmp/'.format(bucket)

meta = datatypes.create_spark_schema_from_metadata_file(meta_path)
df_old = spark.read.csv(csv_path, header = True, schema=meta)
df_old = drd.init_dea_record_datetimes(df_old, '2018-01-01 01:00:00')
df_old = df_old.withColumn('partition_bin', F.floor(df_old.diamond_id / 10000).cast(IntegerType()))
df_old.write.mode('overwrite').partitionBy('partition_bin').parquet(db_table_path)

df_new = df_old.filter('partition_bin = 0')
df_new = drd.init_dea_record_datetimes(df_new, '2018-01-01 02:00:00')

uto.insert_df_into_table_and_update_scd_by_row(spark, new_df=df_new, table_db_base_path = db_table_path, partition_path='partition_bin=0')

spark.read.parquet(db_table_path_tmp).createOrReplaceTempView('df')
test1 = spark.sql("SELECT DISTINCT dea_record_start_datetime, dea_record_end_datetime FROM df")
test2 = spark.sql("SELECT DISTINCT dea_record_start_datetime, dea_record_end_datetime FROM df where dea_record_update_type='new'")
test3 = spark.sql("SELECT DISTINCT dea_record_start_datetime, dea_record_end_datetime FROM df where dea_record_update_type='old'")

if sorted(test1.collect()) != sorted(test1_ans.collect()) :
    raise ValueError("insert_df_into_table_and_update_scd_by_row FAILURE")
if sorted(test2.collect()) != sorted(test2_ans.collect()) :
    raise ValueError("insert_df_into_table_and_update_scd_by_row FAILURE")
if sorted(test3.collect()) != sorted(test3_ans.collect()) :
    raise ValueError("insert_df_into_table_and_update_scd_by_row FAILURE")
print("===> insert_df_into_table_and_update_scd_by_row ===> OK")

### Test write_update_from_tmp_to_table_db (3/3) ###
test4_ans = spark.createDataFrame([Row(dea_record_start_datetime=datetime.datetime(2018, 1, 1, 1, 0), dea_record_end_datetime=datetime.datetime(2018, 1, 1, 2, 0)),
                                   Row(dea_record_start_datetime=datetime.datetime(2018, 1, 1, 1, 0), dea_record_end_datetime=datetime.datetime(2999, 1, 1, 0, 0)),
                                   Row(dea_record_start_datetime=datetime.datetime(2018, 1, 1, 2, 0), dea_record_end_datetime=datetime.datetime(2999, 1, 1, 0, 0))]).select('dea_record_start_datetime', 'dea_record_end_datetime')

test5_ans = spark.createDataFrame([Row(dea_record_start_datetime=datetime.datetime(2018, 1, 1, 1, 0), dea_record_end_datetime=datetime.datetime(2018, 1, 1, 2, 0)),
                                   Row(dea_record_start_datetime=datetime.datetime(2018, 1, 1, 2, 0), dea_record_end_datetime=datetime.datetime(2999, 1, 1, 0, 0))]).select('dea_record_start_datetime', 'dea_record_end_datetime')

test6_ans = spark.createDataFrame([Row(dea_record_start_datetime=datetime.datetime(2018, 1, 1, 1, 0), dea_record_end_datetime=datetime.datetime(2999, 1, 1, 0, 0))]).select('dea_record_start_datetime', 'dea_record_end_datetime')

uto.write_update_from_tmp_to_table_db(spark, db_table_path, partition_path = 'partition_bin=0/', coalesce_size = 4)
if spark.read.parquet(db_table_path).count() != 63940 :
    raise ValueError('write_update_from_tmp_to_table_db FAILURE')

spark.read.parquet(db_table_path).createOrReplaceTempView('df')
test4 = spark.sql("SELECT DISTINCT dea_record_start_datetime, dea_record_end_datetime FROM df")
if sorted(test4.collect()) != sorted(test4_ans.collect()) :
    raise ValueError("write_update_from_tmp_to_table_db FAILURE")
    
test5 = spark.sql("SELECT DISTINCT dea_record_start_datetime, dea_record_end_datetime FROM df where partition_bin = 0")
if sorted(test5.collect()) != sorted(test5_ans.collect()) :
    raise ValueError("write_update_from_tmp_to_table_db FAILURE")
    
test6 = spark.sql("SELECT DISTINCT dea_record_start_datetime, dea_record_end_datetime FROM df where partition_bin <> 0")
if sorted(test6.collect()) != sorted(test6_ans.collect()) :
    raise ValueError("write_update_from_tmp_to_table_db FAILURE")
print("===> write_update_from_tmp_to_table_db (3/3) ===> OK")

job.commit()