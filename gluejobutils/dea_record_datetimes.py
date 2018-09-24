# -*- coding: utf-8 -*-
import csv
import json
import os
import boto3
import botocore

from gluejobutils.s3 import s3_path_to_bucket_key, folder_contains_only_files_with_extension
from gluejobutils.utils import remove_slash, add_slash, list_to_sql_select, standard_datetime_format, standard_datetime_format_python

# Need to be able to run script on glue (using 2.7) or on deployed docker (using 3.6)
try :
    # python 2.7
    from StringIO import StringIO
except ImportError:
    # python 3.6
    from io import StringIO

# No pyspark module if running on standard python docker so skip if need be
try :
    from pyspark.sql.types import *
    import pyspark.sql.functions as F
except ImportError:
    pass

from itertools import chain

static_record_end_datetime = '2999-01-01 00:00:00'

def set_dea_record_start_datetime(df, datetime_string, datetime_format=standard_datetime_format) :
    """
    Set a start datetime to a df as the dea_record_start_datetime. datetime_string should be a string representing datetime (default format is 'yyyy-MM-dd HH:mm:ss').
    For non standard format to datetime_string use datetime_format to put it in standard format.
    Note that the datetime_format specified should be a standard Java datetime format and not the python datetime format.
    """
    df = df.withColumn('dea_record_start_datetime', F.to_timestamp(F.lit(datetime_string), datetime_format))
    
    return df

def set_dea_record_end_datetime(df, datetime_string = None) :
    """
    Sets the column dea_record_end_datetime the speficied datetime_string. Default value is to the datetime 2999-01-01 00:00:00.
    Returns a df.
    """
    if datetime_string is None :
        datetime_string = static_record_end_datetime

    df = df.withColumn('dea_record_end_datetime', F.to_timestamp(F.lit(datetime_string), standard_datetime_format))
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