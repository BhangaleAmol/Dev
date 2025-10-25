import json
import requests
import logging
import inspect


def get_record(storage_name, storage_key, table_name, partition_key, row_key) -> dict:

    try:
        logger = logging.getLogger('edm')

        header = {
            'Content-type': 'application/json',
            'Accept': 'application/json'
        }
        table_url = f"https://{storage_name}.table.core.windows.net/{table_name}(PartitionKey='{partition_key}',RowKey='{row_key}'){storage_key}"
        response = requests.get(table_url, headers=header, timeout=300)

        if (response.status_code != 200):
            raise Exception(response.text)

        response_obj = json.loads(response.text)

        if 'odata.error' in response_obj:
            logger.debug(
                f'{inspect.stack()[0][3]} exception: {Exception(response_obj)}')
            return None

    except Exception as e:
        logger.debug(f'{inspect.stack()[0][3]} exception: {e}')
        return None

    return json.loads(response.text)


def insert_record(storage_name, storage_key, table_name, row):
    try:
        logger = logging.getLogger('edm')

        partition_key = row["PartitionKey"]
        row_key = row["RowKey"]
        new_record = json.dumps(row)

        header = {
            'Content-type': 'application/json',
            'Accept': 'application/json'
        }
        table_url = f"https://{storage_name}.table.core.windows.net/{table_name}(PartitionKey='{partition_key}',RowKey='{row_key}'){storage_key}"
        response = requests.put(
            table_url, headers=header, data=new_record, timeout=300)

        if (response.status_code not in [200, 204]):
            raise Exception(response.text)

    except Exception as e:
        logger.debug(f'{inspect.stack()[0][3]} exception: {e}')


def update_record(storage_name, storage_key, table_name, row):
    try:
        logger = logging.getLogger('edm')

        partition_key = row["PartitionKey"]
        row_key = row["RowKey"]

        record = get_record(
            storage_name, storage_key, table_name, partition_key, row_key)

        if record is not None:
            new_record = json.dumps({**record, **row})
        else:
            new_record = json.dumps(row)

        header = {
            'Content-type': 'application/json',
            'Accept': 'application/json'
        }
        table_url = f"https://{storage_name}.table.core.windows.net/{table_name}(PartitionKey='{partition_key}',RowKey='{row_key}'){storage_key}"
        response = requests.put(
            table_url, headers=header, data=new_record, timeout=300)

        if (response.status_code not in [200, 204]):
            raise Exception(response.text)

    except Exception as e:
        logger.debug(f'{inspect.stack()[0][3]} exception: {e}')


def delete_record(storage_name, storage_key, table_name, partition_key, row_key):
    try:
        logger = logging.getLogger('edm')

        header = {
            'Content-type': 'application/json',
            'Accept': 'application/json',
            'If-Match': '*'
        }
        table_url = f"https://{storage_name}.table.core.windows.net/{table_name}(PartitionKey='{partition_key}',RowKey='{row_key}'){storage_key}"
        logger.debug(table_url)
        response = requests.delete(table_url, headers=header, timeout=300)

        if (response.status_code not in [200, 204]):
            raise Exception(response.text)

    except Exception as e:
        logger.debug(f'{inspect.stack()[0][3]} exception: {e}')
