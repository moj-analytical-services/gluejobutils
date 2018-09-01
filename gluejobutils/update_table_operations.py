# -*- coding: utf-8 -*-
import os

from gluejobutils.s3 import s3_path_to_bucket_key, folder_contains_only_files_with_extension
from gluejobutils.utils import remove_slash, add_slash, list_to_sql_select

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
except ImportError:
    pass

from itertools import chain

def insert_df_into_table_and_update_scd_by_row(spark, new_df, table_db_base_path, update_by_cols, partition_path = '', coalesce_size = 4, update_latest_records_only = True, write_back_table_db_path = False) :
    """
    Updates the data that current exists at table_db_base_path with new_df.

    new_df should have the exact same columns and meta as table_db_base_path (therefore record start and end dates should have been
    applied to new_df.

    table at table_db_base_path is read in and joined to new_df on update_by_cols. Any rows from table_db_base_path 
    that do not match to new_df are written to a partition called dea_record_update_type (set to old). These are records that are unchanged. 

    The rest of table_db_base_path is appended to new_df as these rows also exist in new_df so we need to recalculate SCD2.

    The SCD2 (specifically dea_record_start_date and dea_record_end_date) are recalculated for the appended df. SCD2 are recalculated
    using the update_by_cols variable. This data is then written the same tmp directory but with the partition dea_record_update_type set to new.

    If no file exists in table_db_base_path then this function writes new_df straight to the tmp folder with partition dea_record_update_type set to new.

    update_latest_records_only means that only latest records are called from current table. Set to false if you wish to back fill old update.
    """
    if not isinstance(update_by_cols, list) :
        raise TypeError('update_by_cols must be a list of column names')

    bucket, key = s3_path_to_bucket_key(table_db_base_path)
    table_base_path, table_name = os.path.split(remove_slash(key))

    table_db_path = add_slash(os.path.join(table_db_base_path, partition_path))
    tmp_table_db_path = os.path.join(table_base_path, table_name + '_tmp', partition_path)
    tmp_table_db_path_old_partition = add_slash(os.path.join('s3://', bucket, tmp_table_db_path, 'dea_record_update_type=old'))
    tmp_table_db_path_new_partition = add_slash(os.path.join('s3://', bucket, tmp_table_db_path, 'dea_record_update_type=new'))

    if folder_contains_only_files_with_extension(table_db_path, '.parquet') :
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

def insert_df_into_table_and_update_scd_by_partition_block(spark, new_df, table_db_base_path, partition_path, coalesce_size = 4) :
    """
    Replaces a data chunk in the partition path and sets all dea_record_start_datetimes to the dea_record_start_datetime in new_df.
    This function expects all rows in new_df to have the same dea_record_start_datetime and dea_record_end_datetime (this must be set to the static_record_end_datetime i.e. new data - this function does not work for late arriving facts) 
    """
    
    new_df.createOrReplaceTempView('new_df')
    df_distinct_rows = spark.sql("SELECT DISTINCT dea_record_start_datetime, dea_record_end_datetime FROM new_df").collect()
    dea_dict = df_distinct_rows[0].asDict()

    if len(df_distinct_rows) != 1 :
        raise ValueError("new_df must have the same dea_record_start_datetime for all rows")
    if dea_dict['dea_record_end_datetime'].strftime(standard_datetime_format_python) != static_record_end_datetime :
        raise ValueError("new_df must have a dea_record_satrt_datetime set to " + static_record_end_datetime)
        
    bucket, key = s3_path_to_bucket_key(table_db_base_path)
    table_base_path, table_name = os.path.split(remove_slash(key))
    
    table_db_path = add_slash(os.path.join(table_db_base_path, partition_path))
    
    tmp_table_db_path = os.path.join(table_base_path, table_name + '_tmp', partition_path)
    tmp_table_db_path_old_partition = add_slash(os.path.join('s3://', bucket, tmp_table_db_path, 'dea_record_update_type=old'))
    tmp_table_db_path_new_partition = add_slash(os.path.join('s3://', bucket, tmp_table_db_path, 'dea_record_update_type=new'))

    if folder_contains_only_files_with_extension(table_db_path, '.parquet') :
        current_df = spark.read.parquet(table_db_path)
        current_df_non_latest = current_df.filter("dea_record_end_datetime <> to_timestamp('{}', '{}')".format(static_record_end_datetime, standard_datetime_format))
        current_df_latest = current_df.filter("dea_record_end_datetime = to_timestamp('{}', '{}')".format(static_record_end_datetime, standard_datetime_format))

        # Set latest rows of current df's end date to the new_df start date
        current_df_latest = set_dea_record_end_datetime(current_df_latest, dea_dict['dea_record_start_datetime'].strftime(standard_datetime_format_python))
        
        # combine current_df back together
        current_df = current_df_latest.union(current_df_non_latest)

        # Write current df to old partition
        current_df.coalesce(coalesce_size).write.mode('overwrite').format('parquet').save(tmp_table_db_path_old_partition)
        new_df.coalesce(coalesce_size).write.mode('overwrite').format('parquet').save(tmp_table_db_path_new_partition)

    else :
        # Write data to new partition
        new_df.coalesce(coalesce_size).write.mode('overwrite').format('parquet').save(tmp_table_db_path_new_partition)

def write_update_from_tmp_to_table_db(spark, table_db_base_path, partition_path = '', coalesce_size = 4) :
    """
    Takes data from a specific path and partition and write data in the tmp version of this path and writes it to the specific path.
    Used after upsert_table_partition_with_new_df or upsert_table_by_record
    """

    bucket, key = s3_path_to_bucket_key(table_db_base_path)
    table_base_path, table_name = os.path.split(remove_slash(key))
    
    table_db_path = add_slash(os.path.join(table_db_base_path, partition_path))
    tmp_table_db_path = add_slash(os.path.join('s3://', bucket, table_base_path, table_name + '_tmp', partition_path))

    # Read in new data and drop dea_record_update_type
    tab = spark.read.parquet(tmp_table_db_path).cache().drop('dea_record_update_type')

    # Seperate data into nicer chunks
    tab.coalesce(coalesce_size).write.mode('overwrite').format('parquet').save(table_db_path)