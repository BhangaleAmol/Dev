
from azure.cosmosdb.table.tableservice import TableService
import getopt
import sys
import importlib
import os
import time


def save_config_table(account_name, account_key, table_name, config):
    table_service = TableService(
        account_name=account_name, account_key=account_key)

    sys.stdout.write('Saving config table {0}'.format(table_name))

    attempts = 10
    for row in config:
        for count in range(attempts):
            try:
                table_service.insert_or_merge_entity(table_name, row)
            except Exception as e:
                if count == (attempts - 1):
                    raise(e)
                time.sleep(3)
                continue
            else:
                break

def create_config_table(account_name, account_key, table_name):
    table_service = TableService(
        account_name=account_name, account_key=account_key)
    
    sys.stdout.write('Creating table {0}'.format(table_name))

    attempts = 30
    table_created = False
    for count in range(attempts):
        try:
            table_created = table_service.create_table(table_name, fail_on_exist=False)
            if table_created:
                time.sleep(20)

        except Exception as e:
            if count == (attempts - 1):
                raise(e)
            time.sleep(60)
            continue
        else:
            break    

def read_config_file(table_name):
    module = importlib.import_module(table_name)
    config = module.config

    result = []
    for row in config:
        if 'PUBLISH' not in row:
            result.append(row)
        elif row['PUBLISH']:
            result.append(row)
    return result

if __name__ == '__main__':
    try:
        opts, args = getopt.getopt(
            sys.argv[1:], 
            's:k:p',
            ['storage_name=', 'storage_key=', 'config_path=']
        )
    except getopt.GetoptError:
        print(
            'deploy_config_table.py -s <storage_name> -k <storage_key> -p <config_path>)')
        sys.exit(2)

    for opt, arg in opts:
        if opt in ('-s', '--storage_name'):
            storage_name = arg
        elif opt in ('-k', '--storage_key'):
            storage_key = arg
        elif opt in ('-p', '--config_path'):
            config_path = arg

    sys.path.insert(0, config_path)
    _, _, file_names = next(os.walk(config_path))

    for file_name in file_names:
        table_name = os.path.splitext(file_name)[0]
        sys.stdout.write(table_name)
        config = read_config_file(table_name)
        create_config_table(storage_name, storage_key, table_name)
        save_config_table(storage_name, storage_key, table_name, config)