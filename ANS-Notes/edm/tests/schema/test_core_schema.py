import pytest
from pyspark.sql import SparkSession
from databricks.sdk.runtime import *
from databricks.functions.shared.schema import get_schema_as_dict, get_dict_schema_from_config
from databricks.functions.shared.catalog import get_table_location, get_table_partitions, split_table_name
from databricks.functions.shared.config import config

# table_names = get_tables_for_schema_files('s_core')
table_names = ['s_core.account_col', 's_core.account_ebs', 's_core.account_kgd', 's_core.account_prms',
               's_core.account_sap', 's_core.account_sf', 's_core.account_tot', 's_core.account_agg']


@pytest.fixture(scope='module')
def environment(request):
    environment = request.config.getoption('--environment')
    if environment == 'default':
        environment = config.get('env_name')
    return environment


@pytest.mark.parametrize("table_name", table_names)
def test_core_schema(table_name: str, environment: str):
    table_name = f'edm_{environment}.{table_name}'
    config_schema = get_dict_schema_from_config(table_name)

    spark = SparkSession.builder.getOrCreate()
    df = spark.table(table_name)
    table_schema = get_schema_as_dict(df)

    assert config_schema == table_schema


@pytest.mark.parametrize("table_name", table_names)
def test_core_locations(table_name: str, environment: str):
    table_name = f'edm_{environment}.{table_name}'
    table_path = get_table_location(table_name)

    _, _, s_table_name = split_table_name(table_name)
    expected_path = f'abfss://datalake@edmans{environment}data002.dfs.core.windows.net/datalake/s_core/full_data/{s_table_name}.par'
    assert table_path == expected_path


@pytest.mark.parametrize("table_name", table_names)
def test_core_partitions(table_name: str, environment: str):
    table_name = f'edm_{environment}.{table_name}'
    table_partitions = get_table_partitions(table_name)

    if table_name[-3:] == 'agg':
        expected_partitions = ['_DELETED', '_SOURCE', '_DATE']
    else:
        expected_partitions = ['_DELETED', '_DATE']

    assert expected_partitions == table_partitions
