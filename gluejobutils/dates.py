def convert_all_date_cols_to_string(df):
    """
    Used because Parquet format doesn't yet support the datetype
    """

    date_cols = [f.name for f in df.schema if f.dataType.simpleString() == 'date']
    for dc in date_cols:
        df = df.withColumn(dc, dfs[dc].cast('string'))
    
    return df
