import pandas as pd
import os
import pkg_resources
import json
# import pyspark.sql.types

from dataengineeringutils.utils import read_json

from dataengineeringutils.datatypes import translate_metadata_type_to_type

def translate_metadata_type_to_type(column_type, target_type="glue"):

    io = pkg_resources.resource_stream(__name__, "data/data_type_conversion.json")
    lookup = json.load(io)

    try:
        return lookup[column_type][target_type]
    except:
        raise KeyError("You attempted to lookup column type {}, but this cannot be found in data_type_conversion.csv".format(column_type))


def get_customschema_from_metadata(metadata):

    columns = metadata["columns"]

    custom_schema = pyspark.sql.types.StructType()

    for c in columns:
        this_name = c["name"]
        this_type = translate_metadata_type_to_type(c["type"], "spark")
        this_type = getattr(pyspark.sql.types, this_type)
        this_field = pyspark.sql.types.StructField(this_name, this_type())
        custom_schema.add(this_field)
    return custom_schema