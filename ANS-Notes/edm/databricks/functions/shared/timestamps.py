from databricks.functions.shared.secrets import get_secret
from databricks.functions.shared.config import config
from databricks.functions.shared.catalog import split_table_name
from databricks.functions.shared.azure_table import *
from datetime import datetime, timedelta
import logging
import pyspark.sql.functions as f
import inspect


def get_default_timestamp_record(partition_key, row_key):
    return {
        'PartitionKey': partition_key,
        'RowKey': row_key,
        'MAX_DATETIME': '1900-01-01T00:00:00Z',
        'MAX_DATETIME@odata.type': 'Edm.DateTime',
        'RUN_DATETIME': '1900-01-01T00:00:00Z',
        'RUN_DATETIME@odata.type': 'Edm.DateTime',
        'MAX_SEQUENCE': 0,
        'MAX_SEQUENCE@odata.type': 'Edm.Int32'
    }


def get_cutoff_value(
        table_name,
        source_table=None,
        prune_days=0,
        incremental_type='timestamp',
        date_format='%Y-%m-%dT%H:%M:%S',
        config=config):

    record = {}
    record = get_timestamp_record(table_name, source_table, config=config)

    if prune_days is None:
        prune_days = 0

    # timestamp
    if incremental_type == 'timestamp':

        cutoff_datetime_dt = datetime.strptime(
            '1900-01-01T00:00:00Z', '%Y-%m-%dT%H:%M:%SZ')

        max_datetime = record["MAX_DATETIME"]

        try:
            max_datetime_dt = datetime.strptime(
                max_datetime, '%Y-%m-%dT%H:%M:%SZ')
        except:
            max_datetime_dt = datetime.strptime(
                max_datetime, '%Y-%m-%dT%H:%M:%S.%fZ')

        if max_datetime != '1900-01-01T00:00:00Z':
            cutoff_datetime_dt = max_datetime_dt + timedelta(days=-prune_days)

        cutoff_value = cutoff_datetime_dt.strftime(date_format)
        cutoff_value = cutoff_value.replace('T', ' ')

    # sequence
    if incremental_type == 'sequence':
        max_sequence = record["MAX_SEQUENCE"]
        cutoff_value = max_sequence

    logger = logging.getLogger('edm')
    logger.debug(
        f'{inspect.stack()[0][3]} cutoff_value: {cutoff_value}, prune_days: {str(prune_days)}')

    return cutoff_value


def get_max_value(df, incremental_column, incremental_type='timestamp'):

    max_value = None

    if (incremental_type == 'timestamp'):
        default_value = '1900-01-01T00:00:00'
    else:
        default_value = 0

    if incremental_column not in df.columns:
        message = f"Column {incremental_column} not present in the dataset"
        raise Exception(message)

    data_type = str(df.schema[incremental_column].dataType)

    if data_type == 'TimestampType()':
        max_value = (
            df.select(
                f.date_format(
                    f.max(f.col(incremental_column)),
                    'yyyy-MM-dd HH:mm:ss'
                )
                .alias("max")
            )
            .limit(1).collect()[0].max
        )

    elif data_type == 'DateType()':
        max_value = (
            df.select(
                f.date_format(
                    f.max(f.col(incremental_column)),
                    'yyyy-MM-dd'
                )
                .alias("max")
            )
            .limit(1).collect()[0].max
        )

    else:
        max_value = (
            df.select(
                f.max(f.col(incremental_column)).alias("max")
            )
            .limit(1).collect()[0].max
        )

    if max_value is None:
        max_value = default_value

    return str(max_value)


def get_timestamp_record(table_name, source_table=None, config=config) -> dict:

    partition_key, row_key = get_key_values(table_name, source_table)

    logger = logging.getLogger('edm')
    logger.debug(
        f'{inspect.stack()[0][3]} partition_key: {partition_key}, row_key: {row_key}')

    storage_name = config.get('cfg_storage_name')
    secret_name = config.get('cfg_storage_secret_name')
    cfg_table_name = config.get('timestamp_table_name')

    storage_key = get_secret(secret_name)
    record = get_record(storage_name, storage_key,
                        cfg_table_name, partition_key, row_key)

    if record is None:
        default_record = get_default_timestamp_record(partition_key, row_key)
        insert_record(storage_name, storage_key,
                      cfg_table_name, default_record)
        record = default_record
    return record


def get_key_values(table_name, source_table=None):

    _, database_name, s_table_name = split_table_name(table_name)

    partition_key = database_name.lower()
    row_key = s_table_name if source_table is None else f"{s_table_name} [{source_table}]"
    row_key = row_key.lower()
    return (partition_key, row_key)


def update_cutoff_value(cutoff_value, table_name, source_table=None, incremental_type='timestamp', config=config):

    storage_name = config.get('cfg_storage_name')
    secret_name = config.get('cfg_storage_secret_name')
    cfg_table_name = config.get('timestamp_table_name')

    partition_key, row_key = get_key_values(table_name, source_table)
    storage_key = get_secret(secret_name)

    data = {}
    data['PartitionKey'] = partition_key
    data['RowKey'] = row_key

    if cutoff_value is not None and incremental_type == "timestamp":
        data['MAX_DATETIME'] = cutoff_value.replace(' ', 'T')
        data['MAX_DATETIME@odata.type'] = 'Edm.DateTime'

    if cutoff_value is not None and incremental_type == "sequence":
        data['MAX_SEQUENCE'] = cutoff_value
        data['MAX_SEQUENCE@odata.type'] = 'Edm.Int32'

    logger = logging.getLogger('edm')
    logger.debug(f'{inspect.stack()[0][3]} partition_key: {partition_key}')
    logger.debug(f'{inspect.stack()[0][3]} row_key: {row_key}')
    logger.debug(f'{inspect.stack()[0][3]} cutoff_value: {cutoff_value}')

    update_record(storage_name, storage_key, cfg_table_name, data)


def update_run_datetime(run_datetime, table_name, source_table=None, config=config):

    storage_name = config.get('cfg_storage_name')
    secret_name = config.get('cfg_storage_secret_name')
    cfg_table_name = config.get('timestamp_table_name')

    partition_key, row_key = get_key_values(table_name, source_table)
    storage_key = get_secret(secret_name)

    data = {}
    data['PartitionKey'] = partition_key
    data['RowKey'] = row_key
    data['RUN_DATETIME'] = run_datetime.replace(' ', 'T')
    data['RUN_DATETIME@odata.type'] = 'Edm.DateTime'

    logger = logging.getLogger('edm')
    logger.debug(f'{inspect.stack()[0][3]} partition_key: {partition_key}')
    logger.debug(f'{inspect.stack()[0][3]} row_key: {row_key}')
    logger.debug(f'{inspect.stack()[0][3]} run_datetime: {run_datetime}')

    update_record(storage_name, storage_key, cfg_table_name, data)


def remove_timestamp_record(table_name, source_table=None, config=config):

    partition_key, row_key = get_key_values(table_name, source_table)

    logger = logging.getLogger('edm')
    logger.debug(
        f'{inspect.stack()[0][3]} partition_key: {partition_key}, row_key: {row_key}')

    storage_name = config.get('cfg_storage_name')
    secret_name = config.get('cfg_storage_secret_name')
    cfg_table_name = config.get('timestamp_table_name')

    storage_key = get_secret(secret_name)
    delete_record(storage_name, storage_key,
                  cfg_table_name, partition_key, row_key)
