import os
import pkg_resources
import json
import pyspark.sql.types

from gluejobutils.s3 import read_json_from_s3
from gluejobutils.utils import read_json

def translate_metadata_type_to_type(column_type, target_type="glue"):

    io = pkg_resources.resource_stream(__name__, "data/data_type_conversion.json")
    lookup = json.load(io)

    try:
        return lookup[column_type][target_type]
    except:
        raise KeyError("You attempted to lookup column type {}, but this cannot be found in data_type_conversion.csv".format(column_type))


def create_spark_schema_from_metadata(metadata, exclude_cols = [], non_nullable_cols = []):

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
    if 's3://' in filepath :
        meta = 
    columns = [c for c in metadata["columns"] if c["name"] not in exclude_cols]

    custom_schema = pyspark.sql.types.StructType()

    for c in columns:
        this_name = c["name"]
        nullable = this_name not in non_nullable_cols
        this_type = getattr(pyspark.sql.types, translate_metadata_type_to_type(c["type"], "spark"))
        this_field = pyspark.sql.types.StructField(this_name, this_type(), nullable)
        custom_schema.add(this_field)
        
    return custom_schema

def create_spark_schema_from_meta_dict(meta, exclude_cols = None) :
    if meta_to_spark_type_converter is None :
        meta_to_spark_type_converter = default_meta_to_spark_type_converter

    meta_cols = meta['columns']

    fields = []
    
    if exclude_cols :
        meta_cols = [m for m in meta_cols]
    for mc in meta_cols :
        fields.append(StructField(mc['name'], meta_to_spark_type_converter[mc['type']], True))

    return StructType(fields)

def create_spark_schema_from_meta_file(filepath, meta_to_spark_type_converter = None, exclude_cols = None) :
    meta = read_json(filepath)
    return create_spark_schema_from_meta_dict(meta, meta_to_spark_type_converter, exclude_cols=exclude_cols)

