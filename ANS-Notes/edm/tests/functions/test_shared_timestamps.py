from databricks.functions.shared.tests import *
from databricks.functions.shared.config import config
from databricks.functions.shared.timestamps import *
import pytest
import inspect
import time


@pytest.fixture(scope='module')
def database_name(request):
    return request.config.getoption('--database_name')


@pytest.fixture(scope='module')
def test_config():
    return {
        'cfg_storage_name': config['cfg_storage_name'],
        'cfg_storage_secret_name': config['cfg_storage_secret_name'],
        'timestamp_table_name': 'tests'
    }


def test_get_timestamp_record(test_config, database_name):
    table_name = f'{database_name}.{inspect.stack()[0][3]}'
    source_table = 'source_table'

    actual_record = get_timestamp_record(
        table_name, source_table, config=test_config)
    [actual_record.pop(c, None)
     for c in ['odata.metadata', 'odata.etag', 'Timestamp', 'MAX_SEQUENCE@odata.type']]

    expected_record = {
        'PartitionKey': database_name,
        'RowKey': 'table_name [source_table]',
        'MAX_DATETIME': '1900-01-01T00:00:00Z',
        'MAX_DATETIME@odata.type': 'Edm.DateTime',
        'RUN_DATETIME': '1900-01-01T00:00:00Z',
        'RUN_DATETIME@odata.type': 'Edm.DateTime',
        'MAX_SEQUENCE': 0
        # 'MAX_SEQUENCE@odata.type': 'Edm.Int32' # rest api does not return this column
    }

    remove_timestamp_record(table_name, source_table, config=test_config)
    assert set(actual_record) == set(expected_record)


def test_get_cutoff_value(test_config, database_name):
    table_name = f'{database_name}.{inspect.stack()[0][3]}'
    source_table = 'source_table'

    cutoff_value = get_cutoff_value(
        table_name, source_table, prune_days=0, config=test_config)

    remove_timestamp_record(table_name, source_table, config=test_config)
    assert cutoff_value == '1900-01-01 00:00:00'


def test_update_cutoff_value(test_config, database_name):
    table_name = f'{database_name}.{inspect.stack()[0][3]}'
    source_table = 'source_table'

    update_cutoff_value('1901-01-01 00:00:00', table_name,
                        source_table, config=test_config)
    time.sleep(5)
    cutoff_value = get_cutoff_value(
        table_name, source_table, prune_days=0, config=test_config)

    remove_timestamp_record(table_name, source_table, config=test_config)
    assert cutoff_value == '1901-01-01 00:00:00'


def test_update_run_datetime(test_config, database_name):
    table_name = f'{database_name}.{inspect.stack()[0][3]}'
    source_table = 'source_table'

    update_run_datetime('1906-01-01T00:00:00', table_name,
                        source_table, config=test_config)
    time.sleep(5)
    actual_record = get_timestamp_record(
        table_name, source_table, config=test_config)

    remove_timestamp_record(table_name, source_table, config=test_config)
    assert actual_record['RUN_DATETIME'] == '1906-01-01T00:00:00Z'
