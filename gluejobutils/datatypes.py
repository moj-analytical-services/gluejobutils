import os
import pkg_resources
import json
import pyspark
import pyspark.sql.functions as F

from gluejobutils.s3 import read_json_from_s3
from gluejobutils.utils import read_json, standard_date_format, standard_datetime_format


def translate_metadata_type_to_type(column_type, target_type="glue"):
    """
    Uses json stored in package to convert generic meta data types to glue or spark data types.
    """
    io = pkg_resources.resource_stream(__name__, "data/data_type_conversion.json")
    lookup = json.load(io)

    try:
        return lookup[column_type][target_type]
    except:
        raise KeyError(
            "You attempted to lookup column type {}, but this cannot be found in data_type_conversion.json".format(
                column_type
            )
        )


def create_spark_schema_from_metadata(metadata, drop_columns=[], non_nullable_cols=[]):
    """
    Creates a spark schema from a meta data dictionary.
    Excluding any column names that are in the drop_columns list (default is an empty list).
    To specify any columns that should not be nullable list their names in non_nullable_cols (default is an empty list). 
    """
    columns = [c for c in metadata["columns"] if c["name"] not in drop_columns]

    custom_schema = pyspark.sql.types.StructType()

    for c in columns:
        this_name = c["name"]
        nullable = this_name not in non_nullable_cols
        this_type = getattr(
            pyspark.sql.types, translate_metadata_type_to_type(c["type"], "spark")
        )
        this_field = pyspark.sql.types.StructField(this_name, this_type(), nullable)
        custom_schema.add(this_field)

    return custom_schema


def create_spark_schema_from_metadata_file(
    filepath, drop_columns=[], non_nullable_cols=[]
):
    """
    Creates a spark schema from a json file that is a meta data dictionary. If filepath starts with s3:// the 
    function assumes it is an S3 file otherwise it tries to read the file from the local directory.
    """
    if "s3://" in filepath:
        metadata = read_json_from_s3(filepath)
    else:
        metadata = read_json(filepath)
    return create_spark_schema_from_metadata(
        metadata, drop_columns=drop_columns, non_nullable_cols=non_nullable_cols
    )


def align_df_to_meta(
    df, meta, ignore_columns=[], drop_columns=[], null_missing_cols=False
):

    """
    Casts the columns in dataframe provided to the meta data dictionary provided.
    df : Spark DataFrame
    meta : meta data dictionary
    ignore_columns : a list of column names to not cast to the meta data dictionary. These columns are remained unchanged.
    drop_columns : Removes these columns from the dataframe
    null_mussing_cols : If a column in the meta dictionary does not exist in the dataframe then a column of nulls matching the information in the meta data will be added to the dataframe.
    """
    all_exclude_cols = ignore_columns + drop_columns
    meta_cols_to_convert = [
        m for m in meta["columns"] if m["name"] not in all_exclude_cols
    ]
    df_cols = df.columns
    for m in meta_cols_to_convert:
        this_type = getattr(
            pyspark.sql.types, translate_metadata_type_to_type(m["type"], "spark")
        )
        if m["name"] in df_cols:
            df = df.withColumn(m["name"], df[m["name"]].cast(this_type()))
        elif null_missing_cols:
            df = df.withColumn(m["name"], F.lit(None).cast(this_type()))
        else:
            raise ValueError(
                "ETL_ERROR: Column name in meta ({}) not in dataframe. Set null_missing_cols to True if you wish to null missing cols. Columns in dataframe {}".format(
                    m["name"], ", ".join(df_cols)
                )
            )

    df = df.select(
        [x["name"] for x in meta["columns"] if x["name"] not in drop_columns]
    )

    return df
