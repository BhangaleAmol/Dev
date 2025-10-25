from pyspark.sql import SparkSession
from databricks.functions.shared import *
import databricks.functions.shared.transformations as t
import databricks.functions.shared.validation as v


def read_data(file_path):
    spark = SparkSession.builder.getOrCreate()
    return {
        'df': spark.read.format('parquet').load(file_path)
    }


def transform_data(datasets):
    return (
        datasets['df']
        .transform(t.add_internal_columns)
        .transform(t.fix_column_names)
        .transform(t.trim_all_string_values)
        .transform(t.sort_internal_columns_first)
        .transform(t.cast_null_data_type_to_string)
        .execute(v.check_count_non_zero)
        .distinct()
    )


def save_data(df, params: dict):
    create_database(params.get('database_name'))

    options = {'partition_columns': params.get('partition_columns')}
    create_table(
        params.get('table_name'),
        df.schema,
        params.get('table_path'), options)

    options = {
        'append_only': params.get('append_only'),
        'key_columns': params.get('key_columns'),
        'overwrite': params.get('overwrite'),
        'partition_columns': params.get('partition_columns')
    }
    save_to_table(df, params.get('table_name'), options)
