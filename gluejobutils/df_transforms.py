#   Module for some useful dataframe transforms
from itertools import chain
import pyspark.sql.functions as F


def apply_overwrite_dict_to_df(df, lookup_col, overwrite_dict):
    """
    df : A spark dataframe
    lookup_col : The column name that should be used to apply fixes (e.g. col1)
    overwrite_dict : should be a dictionary where each key is the value of lookup_col you want to fix. (e.g. {'a' : {'col2' : 2}} to fix the value in col2 when col1 is equal to 'a')
    """
    # Split overwrite_dict into a dictionary of single key value pairs dictionaries
    skvp = {}
    for k in overwrite_dict:
        for kk in overwrite_dict[k]:
            if kk not in skvp:
                skvp[kk] = {}

            skvp[kk][k] = overwrite_dict[k][kk]

    # for each col that is going to be overwritten apply the single key value pairs
    for k in skvp:
        mapping_expr = F.create_map([F.lit(x) for x in chain(*skvp[k].items())])
        df = df.withColumn(
            k,
            F.when(mapping_expr.getItem(df[lookup_col]).isNull(), df[k]).otherwise(
                mapping_expr.getItem(df[lookup_col])
            ),
        )

    return df
