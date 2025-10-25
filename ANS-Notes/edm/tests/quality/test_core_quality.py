import pytest
from pyspark.sql import SparkSession
from databricks.sdk.runtime import *

table_names = ['s_core.account_col', 's_core.account_ebs', 's_core.account_kgd', 's_core.account_prms',
               's_core.account_sap', 's_core.account_sf', 's_core.account_tot', 's_core.account_agg']


@pytest.fixture(scope='module')
def environment(request):
    environment = request.config.getoption('--environment')
    if environment == 'default':
        environment = None
    return environment


@pytest.mark.parametrize("table_name", table_names)
def test_table_not_empty(table_name: str, environment: str):
    if environment is not None:
        table_name = f'edm_{environment}.{table_name}'
    spark = SparkSession.builder.getOrCreate()
    assert not spark.table(table_name).isEmpty()


@pytest.mark.parametrize("table_name", table_names)
def test_not_all_deleted(table_name: str, environment: str):
    if environment is not None:
        table_name = f'edm_{environment}.{table_name}'
    spark = SparkSession.builder.getOrCreate()
    assert 0 != (
        spark.table(table_name)
        .filter('NOT _DELETED')
        .count()
    )


@pytest.mark.parametrize("table_name", table_names)
def test_modified_not_null(table_name: str, environment: str):
    if environment is not None:
        table_name = f'edm_{environment}.{table_name}'
    spark = SparkSession.builder.getOrCreate()
    assert 0 == (
        spark.table(table_name)
        .filter('_MODIFIED IS NULL')
        .count()
    )


@pytest.mark.parametrize("table_name", table_names)
def test_date_not_null(table_name: str, environment: str):
    if environment is not None:
        table_name = f'edm_{environment}.{table_name}'
    spark = SparkSession.builder.getOrCreate()
    assert 0 == (
        spark.table(table_name)
        .filter('_DATE IS NULL')
        .count()
    )


@pytest.mark.parametrize("table_name", table_names)
def test_source_not_null(table_name: str, environment: str):
    if environment is not None:
        table_name = f'edm_{environment}.{table_name}'
    spark = SparkSession.builder.getOrCreate()
    assert 0 == (
        spark.table(table_name)
        .filter('_SOURCE IS NULL')
        .count()
    )


@pytest.mark.parametrize("table_name", table_names)
def test_unique_id(table_name: str, environment: str):
    if environment is not None:
        table_name = f'edm_{environment}.{table_name}'
    spark = SparkSession.builder.getOrCreate()
    key_df = spark.table(table_name).select('_ID').cache()
    assert key_df.count() == key_df.distinct().count()
