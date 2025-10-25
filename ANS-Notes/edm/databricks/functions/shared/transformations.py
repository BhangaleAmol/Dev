from pyspark.sql import SparkSession
from collections.abc import Callable
from databricks.functions.shared.dataframe import DataFrame
import pyspark.sql.functions as f
import re
import logging


def add_missing_columns(df: DataFrame, columns: list) -> DataFrame:
    missing_columns = set(columns).difference(df.columns)
    for column in missing_columns:
        df = df.select("*", f.lit(None).alias(column))
    return df


def cast_data_type(df, columns, data_type='string'):
    for column in columns:
        df = df.withColumn(column, f.col(column).cast(data_type).alias(column))
    return df


def cast_data_types_from_schema(df: DataFrame, schema: dict) -> DataFrame:
    df_columns = [c.lower() for c in df.columns]
    for column, data_type in schema.items():
        if column.lower() in df_columns:
            df = df.withColumn(column, f.col(column).cast(data_type))
    return df.transform(cast_null_data_type_to_string)


def add_date_column(df, name="_DATE") -> DataFrame:
    return df.withColumn(name, f.current_date())


def add_deleted_column(df, value=False, name="_DELETED") -> DataFrame:
    return df.withColumn(name, f.lit(value))


def add_internal_columns(df, source_name=None) -> DataFrame:

    if source_name is not None:
        df = df.transform(add_source_column, value=source_name)

    return (
        df
        .transform(add_deleted_column)
        .transform(add_modified_column)
        .transform(add_date_column)
    )


def add_modified_column(df: DataFrame, name="_MODIFIED") -> DataFrame:
    return df.withColumn(name, f.current_timestamp())


def add_source_column(df: DataFrame, value='UNKNOWN', name="_SOURCE") -> DataFrame:
    return df.withColumn(name, f.lit(value))


def drop_key_columns(df: DataFrame) -> DataFrame:
    return df.drop(*df.get_key_columns())


def replace_empty_string_with_null(df: DataFrame) -> DataFrame:
    string_columns = [dtype[0] for dtype in df.dtypes if dtype[1] == 'string']

    for column in string_columns:
        df = df.withColumn(
            column,
            f.when(f.trim(f.col(column)) == '', None)
            .otherwise(f.col(column))
        )
    return df


def replace_null_string_with_null(df: DataFrame) -> DataFrame:
    string_columns = [dtype[0] for dtype in df.dtypes if dtype[1] == 'string']

    for column in string_columns:
        df = df.withColumn(
            column,
            f.when(f.trim(f.col(column)) == 'null', None)
            .otherwise(f.col(column))
        )
    return df


def fix_column_names(df: DataFrame) -> DataFrame:
    return df.select(
        [f.col("`" + col + "`").alias(re.sub(r'[^A-Za-z0-9_]+', '', col))
         for col in df.columns]
    )


def fix_dates_below_1900(df: DataFrame) -> DataFrame:
    datetime_columns = [
        dtype[0]
        for dtype in df.dtypes
        if dtype[1] == 'date' or dtype[1] == 'timestamp'
    ]
    for column in datetime_columns:
        df = df.withColumn(
            column,
            f.when(f.col(column) < '1900-01-01', None)
            .otherwise(f.col(column))
        )
    return df


def fix_data_values(df: DataFrame) -> DataFrame:
    return (
        df
        .transform(cast_null_data_type_to_string)
        .transform(fix_dates_below_1900)
        .transform(trim_all_string_values)
        .transform(replace_empty_string_with_null)
        .transform(replace_null_string_with_null)
    )


def cast_null_data_type_to_string(df: DataFrame) -> DataFrame:
    columns = [
        data_type[0]
        for data_type in df.dtypes if data_type[1] == 'null']
    for column in columns:
        df = df.withColumn(column, f.col(column).cast("string"))
    return df


def get_duplicates(df: DataFrame, key_columns: list) -> DataFrame:
    return (
        df
        .join(
            df.groupBy(key_columns).count().where('count = 1').drop('count'),
            on=key_columns,
            how='left_anti'
        )
    )


def attach_unknown_record(df: DataFrame):

    spark = SparkSession.builder.getOrCreate()
    unknown_df = spark.range(1).drop("id")
    for column in df.schema:

        if column.name == "_DELETED":
            unknown_df = unknown_df.withColumn("_DELETED", f.lit(False))

        elif column.name == "_SOURCE":
            unknown_df = unknown_df.withColumn("_SOURCE", f.lit("DEFAULT"))

        elif column.name == "_PART":
            unknown_df = unknown_df.withColumn(
                "_PART", f.to_date(f.lit('1900-01-01')))

        elif str(column.name)[-3:] == '_ID':
            unknown_df = unknown_df.withColumn(column.name, f.lit(0))

        elif str(column.dataType) == 'StringType()':
            unknown_df = unknown_df.withColumn(column.name, f.lit('unknown'))

        elif str(column.dataType) == 'DateType()':
            unknown_df = unknown_df.withColumn(
                column.name, f.to_date(f.lit('1900-01-01')))

        elif str(column.dataType) == 'TimestampType()':
            unknown_df = unknown_df.withColumn(
                column.name, f.to_timestamp(f.lit('1900-01-01 00:00:00')))

        else:
            unknown_df = unknown_df.withColumn(column.name, f.lit(None))

    return df.union(unknown_df)


def attach_record_with_zero(df: DataFrame) -> DataFrame:
    spark = SparkSession.builder.getOrCreate()
    return df.union(spark.createDataFrame([['0']]))


def remove_duplicates(
        df: DataFrame,
        key_columns: list,
        notify_func: Callable[[int, DataFrame, dict], None] = None,
        debug: bool = False) -> DataFrame:

    if debug or notify_func is not None:
        duplicates_df = get_duplicates(df, key_columns)
        duplicates_df.cache()
        duplicates_count = duplicates_df.count()

    if debug:
        logger = logging.getLogger('edm')
        logger.debug(f'remove_duplicates: {duplicates_count} duplicates found')

    if notify_func is not None and duplicates_count > 0:
        duplicates_sample = (
            duplicates_df
            .select(key_columns).distinct().limit(50)
        )
        notify_func(duplicates_count, duplicates_sample)

    return df.dropDuplicates(subset=key_columns)


def sort_internal_columns_first(df: DataFrame) -> DataFrame:
    key_columns = df.get_key_columns()
    key_columns.sort()
    internal_columns = df.get_internal_columns()
    internal_columns.sort()
    data_columns = df.get_data_columns()
    return df.select(key_columns + internal_columns + data_columns)


def sort_columns(df: DataFrame) -> DataFrame:
    columns = df.columns
    columns.sort()
    return df.select(columns)


def trim_all_string_values(df: DataFrame) -> DataFrame:
    str_cols = [dtype[0] for dtype in df.dtypes if dtype[1] == 'string']
    for column in str_cols:
        df = df.withColumn(column, f.trim(f.col(column)))
    return df
