from databricks.sdk.runtime import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, BooleanType, IntegerType, StringType, DateType, TimestampType
from datetime import date
from databricks.functions.shared.tests import *
from databricks.functions.shared.transformations import *


def test_attach_date_column():
    spark = SparkSession.builder.getOrCreate()
    input_df = spark.createDataFrame([("test",)], ['test'])

    expected_schema = StructType([
        StructField("test", StringType(), True),
        StructField("_DATE", DateType(), True),
        StructField("_DATE2", DateType(), True),
    ])

    expected_df = spark.createDataFrame(
        [("test", date.today(), date.today())],
        schema=expected_schema)

    actual_df = (
        input_df
        .transform(add_date_column)
        .transform(add_date_column, '_DATE2')
    )

    assert dataframes_equal(actual_df, expected_df)


def test_attach_deleted_column():
    spark = SparkSession.builder.getOrCreate()
    input_df = spark.createDataFrame([("test",)], ['test'])

    expected_schema = StructType([
        StructField("test", StringType(), True),
        StructField("_DELETED", BooleanType(), True),
        StructField("_DELETED2", BooleanType(), True),
    ])

    expected_df = spark.createDataFrame(
        [("test", False, True)], schema=expected_schema)

    actual_df = (
        input_df
        .transform(add_deleted_column)
        .transform(add_deleted_column, True, '_DELETED2')
    )

    assert dataframes_equal(actual_df, expected_df)


def test_attach_internal_columns():
    spark = SparkSession.builder.getOrCreate()
    input_df = spark.createDataFrame([("test",)], ['test'])

    expected_schema = StructType([
        StructField("test", StringType(), True),
        StructField("_SOURCE", StringType(), True),
        StructField("_DELETED", BooleanType(), True),
        StructField("_MODIFIED", TimestampType(), True),
        StructField("_DATE", DateType(), True),
    ])
    expected_df = spark.createDataFrame([], schema=expected_schema)

    actual_df = (
        input_df
        .transform(add_internal_columns, source_name='TEST')
    )

    assert set(actual_df.columns) == set(expected_df.columns)
    assert set(actual_df.dtypes) == set(expected_df.dtypes)
    assert actual_df.collect()[0]['_SOURCE'] == 'TEST'
    assert actual_df.collect()[0]['_DATE'] == date.today()


def test_attach_modified_column():
    spark = SparkSession.builder.getOrCreate()
    input_df = spark.createDataFrame([("test",)], ['test'])

    expected_schema = StructType([
        StructField("test", StringType(), True),
        StructField("_MODIFIED", DateType(), True),
        StructField("_MODIFIED2", DateType(), True),
    ])

    expected_df = spark.createDataFrame(
        [("test", date.today(), date.today())],
        schema=expected_schema)

    actual_df = (
        input_df
        .transform(add_modified_column)
        .transform(add_modified_column, '_MODIFIED2')
        .withColumn('_MODIFIED', f.to_date(f.col('_MODIFIED')))
        .withColumn('_MODIFIED2', f.to_date(f.col('_MODIFIED')))
    )

    assert dataframes_equal(actual_df, expected_df)


def test_attach_source_column():
    spark = SparkSession.builder.getOrCreate()
    input_df = spark.createDataFrame([("test",)], ['test'])

    expected_schema = StructType([
        StructField("test", StringType(), True),
        StructField("_SOURCE", StringType(), True),
        StructField("_SOURCE2", StringType(), True),
    ])

    expected_df = spark.createDataFrame(
        [("test", 'UNKNOWN', 'SRC')], schema=expected_schema)

    actual_df = (
        input_df
        .transform(add_source_column)
        .transform(add_source_column, 'SRC', '_SOURCE2')
    )

    assert dataframes_equal(actual_df, expected_df)


def test_fix_column_names():
    spark = SparkSession.builder.getOrCreate()
    input_df = spark.createDataFrame(
        [('t', 't', 't')], ['t1!@#', 'T 2', '_t3'])
    expected_df = spark.createDataFrame([('t', 't', 't')], ['t1', 'T2', '_t3'])
    actual_df = input_df.transform(fix_column_names)
    assert set(actual_df.columns) == set(expected_df.columns)


def test_fix_null_data_types():
    spark = SparkSession.builder.getOrCreate()
    input_df = spark.createDataFrame([(None,), ('a',)], ['t1'])

    expected_schema = StructType([
        StructField("t1", StringType(), True),
    ])

    expected_df = spark.createDataFrame(
        [(None,), ('a',)], schema=expected_schema)
    actual_df = input_df.transform(cast_null_data_type_to_string)
    assert set(actual_df.dtypes) == set(expected_df.dtypes)


def test_set_internal_colummns_first():
    spark = SparkSession.builder.getOrCreate()
    input_df = spark.createDataFrame(
        [("_SOURCE", "test", "_ID", "test_ID")],
        ['_SOURCE', 'test', '_ID', 'test_ID', ])

    expected_schema = StructType([
        StructField("_ID", StringType(), True),
        StructField("test_ID", StringType(), True),
        StructField("_SOURCE", StringType(), True),
        StructField("test", StringType(), True),
    ])

    expected_df = spark.createDataFrame(
        [("_ID", "test_ID", "_SOURCE", "test")], schema=expected_schema)

    actual_df = (
        input_df
        .transform(sort_internal_columns_first)
    )

    assert set(actual_df.columns) == set(expected_df.columns)


def test_trim_all_string_values():
    spark = SparkSession.builder.getOrCreate()

    schema = StructType([
        StructField("c1", StringType(), True),
        StructField("c2", StringType(), True),
        StructField("c3", IntegerType(), True),
    ])

    input_df = spark.createDataFrame([(" s1 ", "  s2  ", 1)], schema=schema)
    expected_df = spark.createDataFrame([("s1", "s2", 1)], schema=schema)
    actual_df = input_df.transform(trim_all_string_values)

    assert dataframes_equal(actual_df, expected_df)
