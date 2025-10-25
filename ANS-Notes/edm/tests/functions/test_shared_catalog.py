from pyspark.sql import SparkSession
from databricks.sdk.runtime import *
import pytest
import inspect
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from databricks.functions.shared.tests import *
from databricks.functions.shared.catalog import *
from databricks.functions.shared.storage import *
from databricks.functions.shared.paths import get_test_database_path, get_test_table_path


@pytest.fixture(scope='module')
def database_name(request):
    return request.config.getoption('--database_name')


@pytest.fixture(scope='module', autouse=True)
def setup_database(database_name):
    if database_exists(database_name):
        drop_database(database_name)
    create_database(database_name)


@pytest.fixture(scope="module", autouse=True)
def cleanup(request, database_name):
    def remove_database(database_name):
        if database_exists(database_name):
            drop_database(database_name)
            database_path = get_test_database_path(database_name)
            dbutils.fs.rm(database_path, True)
    request.addfinalizer(lambda: remove_database(database_name))


@pytest.fixture(scope="function")
def create_test_table():
    def _create_test_table(table_name):
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True)
        ])
        spark = SparkSession.builder.getOrCreate()
        df = spark.createDataFrame([(1, 'foo')], schema)
        table_path = get_test_table_path(table_name)
        df.write.option('path', table_path).mode(
            'overwrite').saveAsTable(table_name)
    return _create_test_table


def test_add_column(create_test_table, database_name):
    table_name = f'{database_name}.{inspect.stack()[0][3]}'
    create_test_table(table_name)
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("surname", StringType(), True),
    ])
    spark = SparkSession.builder.getOrCreate()
    expected_df = spark.createDataFrame([(1, 'foo', None), ], schema)
    add_column(table_name, "surname", "string")
    actual_df = spark.table(table_name)
    assert dataframes_equal(actual_df, expected_df)


def test_alter_column_name(create_test_table, database_name):
    table_name = f'{database_name}.{inspect.stack()[0][3]}'
    create_test_table(table_name)
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("surname", StringType(), True)
    ])
    spark = SparkSession.builder.getOrCreate()
    expected_df = spark.createDataFrame([(1, 'foo'), ], schema)
    alter_column_name(table_name, "name", "surname")
    actual_df = spark.table(table_name)
    assert dataframes_equal(actual_df, expected_df)


def test_alter_table_partition(create_test_table, database_name):
    table_name = f'{database_name}.{inspect.stack()[0][3]}'
    create_test_table(table_name)
    alter_table_partition(table_name, ["name"])
    assert set(['name']) == set(get_table_partitions(table_name))


def test_column_exists(create_test_table, database_name):
    table_name = f'{database_name}.{inspect.stack()[0][3]}'
    create_test_table(table_name)
    column_exist = column_exists(table_name, "name")
    column_not_exist = column_exists(table_name, "name2")
    assert column_exist == True
    assert column_not_exist != True


def test_database_exists(database_name):
    assert database_exists(database_name)


def test_drop_table(create_test_table, database_name):
    table_name = f'{database_name}.{inspect.stack()[0][3]}'
    create_test_table(table_name)
    table_path = get_table_location(table_name)
    drop_table(table_name)
    assert not table_exists(table_name)
    assert not file_exists(table_path)


def test_create_table(create_test_table, database_name):
    table_name = f'{database_name}.{inspect.stack()[0][3]}'
    if table_exists(table_name):
        drop_table(table_name)
    create_test_table(table_name)
    assert table_exists(table_name)


def test_get_table_location(create_test_table, database_name):
    table_name = f'{database_name}.{inspect.stack()[0][3]}'
    create_test_table(table_name)
    table_path = get_table_location(table_name)
    expected_path = get_test_table_path(table_name)
    assert table_path == expected_path


def test_get_table_partitions(database_name):
    table_name = f'{database_name}.{inspect.stack()[0][3]}'
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame([(1, 'bar'), ], schema)
    table_path = get_test_table_path(table_name)
    df.write.option('path', table_path) \
        .mode('overwrite').partitionBy('id').saveAsTable(table_name)
    assert set(['id']) == set(get_table_partitions(table_name))


def test_get_table_schema(create_test_table, database_name):
    table_name = f'{database_name}.{inspect.stack()[0][3]}'
    create_test_table(table_name)
    table_schema = get_table_schema(table_name)
    assert {'id': 'int', 'name': 'string'} == table_schema


def test_get_database_name():
    spark = SparkSession.builder.getOrCreate()
    current_catalog = spark.sql(
        'SELECT current_catalog() AS c').collect()[0]['c']
    assert 'cat.db' == get_database_name('cat.db.tab')
    assert 'cat.db' == get_database_name('cat.db')
    assert f"{current_catalog}.db" == get_database_name('db')


def test_get_table_name():
    spark = SparkSession.builder.getOrCreate()
    current_catalog = spark.sql(
        'SELECT current_catalog() AS c').collect()[0]['c']
    current_database = spark.sql(
        'SELECT current_database() AS db').collect()[0]['db']
    assert 'cat.db.tab' == get_table_name('cat.db.tab')
    assert f'{current_catalog}.db.tab' == get_table_name('db.tab')
    assert f'{current_catalog}.{current_database}.tab' == get_table_name('tab')


def test_insert_into_table(create_test_table, database_name):
    table_name = f'{database_name}.{inspect.stack()[0][3]}'
    create_test_table(table_name)

    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame([(2, 'bar'), (3, 'baz')], schema)

    insert_into_table(df, table_name)
    actual_df = spark.table(table_name)
    expected_df = spark.createDataFrame(
        [(1, 'foo'), (2, 'bar'), (3, 'baz')], schema)

    assert dataframes_equal(actual_df, expected_df)


def test_insert_into_table_with_overwrite(create_test_table, database_name):
    table_name = f'{database_name}.{inspect.stack()[0][3]}'
    create_test_table(table_name)

    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame([(1, "bar"), (2, 'baz')], schema)

    insert_into_table(df, table_name, options={'overwrite': True})
    actual_df = spark.table(table_name)
    expected_df = spark.createDataFrame([(1, 'bar'), (2, 'baz')], schema)

    assert dataframes_equal(actual_df, expected_df)


def test_merge_into_table(create_test_table, database_name):
    table_name = f'{database_name}.{inspect.stack()[0][3]}'
    create_test_table(table_name)

    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame([(1, "bar"), (2, 'baz')], schema)

    merge_into_table(df, table_name, ['id'])
    actual_df = spark.table(table_name)
    expected_df = spark.createDataFrame([(1, 'bar'), (2, 'baz')], schema)

    assert dataframes_equal(actual_df, expected_df)


def test_save_to_table_overwrite(create_test_table, database_name):
    table_name = f'{database_name}.{inspect.stack()[0][3]}'
    create_test_table(table_name)

    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("surname", StringType(), True),
    ])
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame([(1, 'bar', 's1'), (2, 'baz', 's2')], schema)

    save_to_table(df, table_name, options={'overwrite': True})
    actual_df = spark.table(table_name)
    expected_df = df

    assert dataframes_equal(actual_df, expected_df)


def test_save_to_table_append(create_test_table, database_name):
    table_name = f'{database_name}.{inspect.stack()[0][3]}'
    create_test_table(table_name)

    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame([(2, 'bar'), (3, 'baz')], schema)

    save_to_table(df, table_name, options={'append_only': True})
    actual_df = spark.table(table_name)
    expected_df = spark.createDataFrame(
        [(1, 'foo'), (2, 'bar'), (3, 'baz')], schema)

    assert dataframes_equal(actual_df, expected_df)


def test_save_to_table_full_no_keys(create_test_table, database_name):
    table_name = f'{database_name}.{inspect.stack()[0][3]}'
    create_test_table(table_name)

    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame([(1, 'bar'), (2, 'baz')], schema)

    save_to_table(df, table_name, options={'incremental': False})
    actual_df = spark.table(table_name)
    expected_df = df

    assert dataframes_equal(actual_df, expected_df)


def test_save_to_table_merge_insert(create_test_table, database_name):
    table_name = f'{database_name}.{inspect.stack()[0][3]}'
    create_test_table(table_name)

    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame([(2, 'bar'), (3, 'baz')], schema)

    save_to_table(df, table_name, options={'key_columns': ['id']})
    actual_df = spark.table(table_name)
    expected_df = spark.createDataFrame(
        [(1, 'foo'), (2, 'bar'), (3, 'baz')], schema)

    assert dataframes_equal(actual_df, expected_df)


def test_save_to_table_merge_update(create_test_table, database_name):
    table_name = f'{database_name}.{inspect.stack()[0][3]}'
    create_test_table(table_name)

    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame([(1, 'bar'), (2, 'baz')], schema)

    save_to_table(df, table_name, options={'key_columns': ['id']})
    actual_df = spark.table(table_name)
    expected_df = spark.createDataFrame([(1, 'bar'), (2, 'baz')], schema)

    assert dataframes_equal(actual_df, expected_df)


def test_table_exists(create_test_table, database_name):
    table_name = f'{database_name}.{inspect.stack()[0][3]}'
    create_test_table(table_name)
    non_existing_table = f'{table_name}_no'
    assert table_exists(table_name)
    assert not table_exists(non_existing_table)
