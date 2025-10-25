def check_count_non_zero(df):
    result = df.count() != 0
    return result


def check_no_duplicates(df, columns):
    if isinstance(columns, str):
        columns = [c.strip() for c in columns.split(',')]

    fields_df = df.select(columns).cache()
    result = fields_df.count() == fields_df.distinct().count()
    return result
