# -*- coding: utf-8 -*-
import csv
import json
import os
import boto3
import botocore

from gluejobutils.s3 import s3_path_to_bucket_key, check_for_parquet_data_in_folder
from gluejobutils.utils import remove_slash, add_slash, list_to_sql_select

import pyspark.sql.functions as F
# Need to be able to run script on glue (using 2.7) or on deployed docker (using 3.6)
try :
    # python 2.7
    from StringIO import StringIO
except :
    # python 3.6
    from io import StringIO

# No pyspark module if running on standard python docker so skip if need be
try :
    from pyspark.sql.types import *
    import pyspark.sql.functions as F
except :
    pass

from itertools import chain

static_record_end_datetime = '2999-01-01 00:00:00'
standard_datetime_format = 'yyyy-MM-dd HH:mm:ss'

def set_dea_record_start_datetime(df, datetime_string, datetime_format=standard_datetime_format) :
    """
    Set a start datetime to a df as the dea_record_start_datetime. datetime_string should be a string representing datetime (default format is 'yyyy-MM-dd HH:mm:ss').
    For non standard format to datetime_string use datetime_format to put it in standard format.
    Note that the datetime_format specified should be a standard Java datetime format and not the python datetime format.
    """
    df = df.withColumn('dea_record_start_datetime', F.to_timestamp(F.lit(datetime_string), datetime_format))
    
    return df

def set_dea_record_end_datetime(df) :
    """
    Sets the column dea_record_end_datetime to the datetime 2999-01-01 00:00:00.
    Returns a df.
    """
    df = df.withColumn('dea_record_end_datetime', F.to_timestamp(F.lit(static_record_end_datetime), standard_datetime_format))
    return df

def init_dea_record_datetimes(df, datetime_string, datetime_format=standard_datetime_format) :
    """
    Initialises dea_record_start_datetime and dea_record_end_datetime.
    Calls set_dea_record_start_datetime and then set_dea_record_end_datetime on df.
    Returns a df.
    """
    df = set_dea_record_start_datetime(df=df, datetime_string=datetime_string, datetime_format=datetime_format)
    df = set_dea_record_end_datetime(df)
    return df

def update_dea_record_end_datetime(df, partition_by, order_by = 'dea_record_start_datetime') :
    """
    Takes dataframe and sets all dea_record_end_datetime by ordering the data by specified order clause and partition variable.
    partition_by and order_by should be a string. If partitioning or ordering by multiple columns provide them as a single comma seperated string e.g. 'col1, col2'.
    dea_record_end_datetime is set to the next records dea_record_start_datetime. If the dea_record_end_datetime is last row of that partition then it is set to 
    the default static_record_end_datetime (i.e. 2999-01-01 00:00:00)
    """
    start_col = 'dea_record_start_datetime' 
    end_col = 'dea_record_end_datetime'
    
    df = df.withColumn(end_col, F.expr("lead({}) OVER (PARTITION BY {} ORDER BY {})".format(start_col, partition_by, order_by)))
    
    # Set nulls to default record end date value
    df = df.withColumn(end_col, F.expr("CASE WHEN {0} IS NULL THEN to_timestamp('{1}', '{2}') ELSE {0} END".format(end_col, static_record_end_datetime, standard_datetime_format)))
    
    # Filter out redundent records
    df = df.filter("{} <> {}".format(start_col, end_col))

    return df

def upsert_table_by_record(spark, new_df, table_db_path, update_by_cols, coalesce_size = 4, update_latest_records_only = True) :
    """
    Updates the data that current exists at table_db_path with new_df.

    new_df should have the exact same columns and meta as table_db_path (therefore record start and end dates should have been
    applied to new_df.

    table at table_db_path is read in and joined to new_df on update_by_cols. Any rows from table_db_path 
    that do not match to new_df are written to a partition called dea_record_update_type (set to old). These are records that are unchanged. 

    The rest of table_db_path is appended to new_df as these rows also exist in new_df so we need to recalculate SCD2.

    The SCD2 (specifically dea_record_start_date and dea_record_end_date) are recalculated for the appended df. SCD2 are recalculated
    using the update_by_cols variable. This data is then written the same tmp directory but with the partition dea_record_update_type set to new.

    If no file exists in table_db_path then this function writes new_df straight to the tmp folder with partition dea_record_update_type set to new.

    update_latest_records_only means that only latest records are called from current table. Set to false if you wish to back fill old update.
    """
    bucket, key = s3_path_to_bucket_key(table_db_path)
    table_base_path, table_name = os.path.split(remove_slash(key))

    tmp_table_db_path = os.path.join(table_base_path, table_name + '_tmp')
    tmp_table_db_path_old_partition = add_slash(os.path.join('s3://', bucket, tmp_table_db_path, 'dea_record_update_type=old'))
    tmp_table_db_path_new_partition = add_slash(os.path.join('s3://', bucket, tmp_table_db_path, 'dea_record_update_type=new'))

    if check_for_parquet_data_in_folder(table_db_path) :
        new_keys = new_df.select(*update_by_cols)
        new_keys.createOrReplaceTempView('update_keys')
        spark.read.parquet(table_db_path).cache().createOrReplaceTempView('current_table')

        ### NB That his is optimised to only add latest records. Will not work when trying to add an older set of records
        update_primary_keys_select = list_to_sql_select(update_by_cols, table_alias = 'update_keys')
        update_concat_keys = 'CONCAT({})'.format(update_primary_keys_select)
        db_primary_keys_select = list_to_sql_select(update_by_cols, table_alias = 'current_table')
        db_concat_keys = 'CONCAT({})'.format(db_primary_keys_select)

        where_statement = "WHERE current_table.dea_record_end_datetime=to_timestamp('{}', '{}')".format(static_record_end_datetime, standard_datetime_format) if update_latest_records_only else ""

        db_join = spark.sql("""
        SELECT  /*+ BROADCAST(update_keys) */ current_table.*,
        ({0} IS NOT NULL) AS inner_flag
        FROM current_table
        LEFT JOIN update_keys
        ON {1} = {0}
        {2}
        """.format(update_concat_keys, db_concat_keys, where_statement))

        # Write unchanged records to old partition
        db_join.filter("NOT inner_flag").drop("inner_flag").coalesce(coalesce_size).write.mode('overwrite').format('parquet').save(tmp_table_db_path_old_partition)

        # Get the rows of the database that matched and add them to the snapshot
        db_update = db_join.filter("inner_flag").drop("inner_flag")
        combined_update = new_df.union(db_update)

        # Apply SCD to update and write to tmp folder 
        combined_update = update_dea_record_end_datetime(combined_update, ','.join(update_by_cols), 'dea_record_start_datetime')
        combined_update.coalesce(coalesce_size).write.mode('overwrite').format('parquet').save(tmp_table_db_path_new_partition)
    else :
        # Write data to new partition
        new_df.coalesce(coalesce_size).write.mode('overwrite').format('parquet').save(tmp_table_db_path_new_partition)


