from pyspark import Row
def apply_cleaning_function_list(pyspark_df, list_of_functions, sampleRatio=None):

    def run_all_functions(row):

        row_dict = row.asDict()

        for fn in list_of_functions:
            row_dict = fn(row_dict)

        return Row(**row_dict)


    new_df = pyspark_df.rdd.map(run_all_functions)
    if sampleRatio:
        return new_df.toDF(sampleRatio=sampleRatio)
    else:
        return new_df.toDF()
