from azure.cosmosdb.table.tableservice import TableService
import sys
import time


def create_table(storage_name, storage_key, table_name):
    table_service = TableService(
        account_name=storage_name, account_key=storage_key)

    print(f'creating table {table_name}')
    sys.stdout.flush()

    attempts = 30
    table_created = False
    for count in range(attempts):
        try:
            table_created = table_service.create_table(
                table_name, fail_on_exist=False)
            if table_created:
                time.sleep(20)

        except Exception as e:
            if count == (attempts - 1):
                raise (e)
            time.sleep(60)
            continue
        else:
            break


def run_query(storage_name, storage_key, table_name, filter, select):
    table_service = TableService(
        account_name=storage_name,
        account_key=storage_key
    )

    return table_service.query_entities(
        table_name,
        filter=filter,
        select=select
    )


def insert_or_merge(rows, storage_name, storage_key, table_name):
    table_service = TableService(
        account_name=storage_name, account_key=storage_key)

    print(f'saving config table {table_name}')
    sys.stdout.flush()

    attempts = 10
    for row in rows:
        for count in range(attempts):
            try:
                table_service.insert_or_merge_entity(table_name, row)
            except Exception as e:
                if count == (attempts - 1):
                    raise (e)
                time.sleep(3)
                continue
            else:
                break


def insert_or_replace(row, storage_name, storage_key, table_name):
    table_service = TableService(
        account_name=storage_name,
        account_key=storage_key)
    table_service.insert_or_replace_entity(table_name, row)
