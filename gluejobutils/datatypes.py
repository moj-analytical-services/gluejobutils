import os
import pkg_resources
import json
import pyspark
import pyspark.sql.functions as F

from gluejobutils.s3 import read_json_from_s3
from gluejobutils.utils import read_json

def translate_metadata_type_to_type(column_type, target_type="glue"):
    """
    Uses json stored in package to convert generic meta data types to glue or spark data types.
    """
    io = pkg_resources.resource_stream(__name__, "data/data_type_conversion.json")
    lookup = json.load(io)

    try:
        return lookup[column_type][target_type]
    except:
        raise KeyError("You attempted to lookup column type {}, but this cannot be found in data_type_conversion.csv".format(column_type))

def create_spark_schema_from_metadata(metadata, exclude_cols = [], non_nullable_cols = []):
    """
    Creates a spark schema from a meta data dictionary.
    Excluding any column names that are in the exclude_cols list (default is an empty list).
    To specify any columns that should not be nullable list their names in non_nullable_cols (default is an empty list). 
    """
    columns = [c for c in metadata["columns"] if c["name"] not in exclude_cols]

    custom_schema = pyspark.sql.types.StructType()

    for c in columns:
        this_name = c["name"]
        nullable = this_name not in non_nullable_cols
        this_type = getattr(pyspark.sql.types, translate_metadata_type_to_type(c["type"], "spark"))
        this_field = pyspark.sql.types.StructField(this_name, this_type(), nullable)
        custom_schema.add(this_field)

    return custom_schema

def create_spark_schema_from_metadata_file(filepath, exclude_cols = [], non_nullable_cols = []):
    """
    Creates a spark schema from a json file that is a meta data dictionary. If filepath starts with s3:// the 
    function assumes it is an S3 file otherwise it tries to read the file from the local directory.
    """
    if 's3://' in filepath :
        metadata = read_json_from_s3(filepath)
    else :
        metadata = read_json(filepath)
    return create_spark_schema_from_metadata(metadata, exclude_cols=exclude_cols, non_nullable_cols=non_nullable_cols)

def align_df_to_meta(df, meta, exclude_columns = [], null_missing_cols = False) :

    """
    Casts the columns in dataframe provided to the meta data dictionary provided
    """
    meta_cols = [m for m in meta['columns'] if m['name'] not in exclude_columns]

    df_cols = df.columns
    for m in meta_cols :
        this_type = getattr(pyspark.sql.types, translate_metadata_type_to_type(m["type"], "spark"))
        if m['name'] in df_cols :
            df = df.withColumn(m['name'], df[m['name']].cast(this_type))
        elif null_missing_cols :
            df = df.withColumn(m['name'], F.lit(None).cast(this_type))
        else :
            raise ValueError("ETL_ERROR: Column name in meta ({}) not in dataframe. Set null_missing_cols to True if you wish to null missing cols. Columns in dataframe {}".format(m['name'], ", ".join(df_cols)))
    
    df = df.select([x['name'] for x in meta_cols])

    return df

def spark_read_csv_using_metadata_path(spark, metadata_path, csv_path, **kwargs) :
    """
    Returns a csv read from S3 using spark. Schema is derived from the meta data.
    """
    schema = create_spark_schema_from_metadata_file(metadata_path)
    df = spark.read.csv(csv_path, schema=schema, **kwargs)
    return df