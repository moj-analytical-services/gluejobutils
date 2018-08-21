def convert_all_date_cols_to_string(df):
    """
    DEPRECATED - parquet now supports dates but keeping for dependencies.

    Used because Parquet format doesn't yet support the datetype.
    """

    date_cols = [f.name for f in df.schema if f.dataType.simpleString() == 'date']
    for dc in date_cols:
        df = df.withColumn(dc, df[dc].cast('string'))
    
    return df
