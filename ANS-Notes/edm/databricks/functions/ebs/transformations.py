from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from databricks.functions.shared.dataframe import DataFrame


def fix_natural_key(df: DataFrame, columns: list) -> DataFrame:
    for column in columns:
        df = df.withColumn(
            column,
            f.regexp_replace(f.col(column).cast('int').cast('string'), ',', ''))
    return df
