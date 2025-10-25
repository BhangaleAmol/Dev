from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from databricks.functions.shared.dataframe import DataFrame


def fix_natural_key(df: DataFrame, columns: list) -> DataFrame:
    for column in columns:
        df = df.withColumn(
            column,
            f.regexp_replace(f.col(column).cast('int').cast('string'), ',', ''))
    return df


def fix_dates(df: DataFrame, columns: list) -> DataFrame:
    for column in columns:
        df = df.withColumn(
            column,
            f.when(f.col(column) == '0001-01-03T00:00:00.000+0000',
                   '1900-01-01T00:00:00.000Z')
            .otherwise(f.col(column))
        )
    return df
