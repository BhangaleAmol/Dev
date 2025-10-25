from pyspark.sql import SparkSession
from databricks.functions.shared.dataframe import DataFrame
import pyspark.sql.functions as f
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import logging
import inspect


def add_hash_key(df: DataFrame, columns: list, name: str = "_ID", default: str = '0') -> DataFrame:
    return (
        df
        .withColumn(
            name,
            f.when(sum(f.col(c).isNull().cast('int')
                   for c in columns) > 0, f.lit(default))
            .otherwise(f.sha2(f.concat_ws('||', *(f.col(c).cast("string") for c in columns)), 256))
        )
    )


def add_key(df: DataFrame, columns: list, name: str, map_table: str) -> DataFrame:

    spark = SparkSession.builder.getOrCreate()
    return (
        df
        .transform(add_hash_key, columns, name="hash")
        .alias('data')
        .join(
            spark.table(map_table).alias('map'),
            f.col('map.hash') == f.col('data.hash'),
            'left'
        )
        .select('data.*', 'map.key')
        .withColumn(name, f.coalesce(f.col('map.key'), f.lit('0')))
        .drop('hash', 'key')
    )


def update_key(table_name: str, columns: list, key_name: str, map_table: str, options={}):

    source_name = options.get('source_name')

    spark = SparkSession.builder.getOrCreate()
    df = (
        spark.table(table_name)
        .filter(f.col('_SOURCE') != 'DEFAULT')
        .filter(f.col(key_name) == '0')
        .filter("_ID <> '0'")  # filter unknown record
    )

    join_filter = "t._ID = s._ID AND t._DELETED = s._DELETED"

    if source_name is not None:
        df = df.filter(f.col('_SOURCE') == source_name)
        join_filter = f"{join_filter} AND t._SOURCE = '{source_name}'"

    df_with_key = (
        df
        .transform(add_key, columns, key_name, map_table)
        .filter(f.col(key_name) != '0')
    )

    logger = logging.getLogger('edm')
    logger.debug(f'{inspect.stack()[0][3]} table_name: {table_name}')
    logger.debug(f'{inspect.stack()[0][3]} key_name: {key_name}')
    logger.debug(f'{inspect.stack()[0][3]} count: {str(df_with_key.count())}')

    deltaTable = DeltaTable.forName(spark, table_name)
    deltaTable.alias("t") \
        .merge(df_with_key.alias("s"), join_filter) \
        .whenMatchedUpdateAll() \
        .execute()


def update_map_table(df: DataFrame, map_table: str, columns: list):

    hash_df = (
        df
        .transform(add_hash_key, columns, name="hash")
        .filter(f.col('_SOURCE') != 'DEFAULT')
        .select(
            'hash',
            f.lower(f.col('_SOURCE')).alias('source'),
            f.concat_ws('|', *columns).alias('nk')
        )
        .distinct()
    ).alias('hash')

    spark = SparkSession.builder.getOrCreate()
    map_df = spark.table(map_table).alias('map')

    max_df = (
        map_df
        .groupBy('source')
        .agg(f.coalesce(f.max('id'), f.lit(0)).alias('max_id'))
    ).alias('max')

    new_map_df = (
        hash_df
        .join(map_df, f.col('map.hash') == f.col('hash.hash'), 'leftanti')
        .join(max_df, f.col('hash.source') == f.col('max.source'), 'left')
        .withColumn(
            'id',
            f.coalesce(f.col('max.max_id'), f.lit(0)) + f.row_number().over(
                Window.partitionBy('hash.source').orderBy('hash.hash')
            )
        )
        .withColumn('key', f.concat(f.col('hash.source'), f.lit('-'), f.col('id')))
        .select('hash.hash', 'hash.source', 'id', 'key', 'nk')
    )

    # https://issues.apache.org/jira/browse/SPARK-14948
    new_map_df = new_map_df.select(
        [f.col(c).alias(c) for c in new_map_df.columns])

    deltaTable = DeltaTable.forName(spark, map_table)
    deltaTable.alias("t") \
        .merge(new_map_df.alias("s"), "t.hash = s.hash") \
        .whenNotMatchedInsertAll() \
        .execute()

    return new_map_df.count()
