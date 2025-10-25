import modules.table as table
import argparse
import os
import json
import csv
from datetime import datetime


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--storage_name")
    parser.add_argument("--storage_key")
    parser.add_argument("--config_folder", default="../configs")
    parser.add_argument("--config_path")
    parser.add_argument("--config_format")
    return parser.parse_args()


def cast_csv_datatypes(config):
    for row in config:
        for column, value in row.items():
            if f'{column}@type' in row.keys():
                data_type = row[f'{column}@type']
                if data_type == 'Boolean':
                    row[column] = value.lower() in ['true']
                if data_type in ['Int32', 'Int64']:
                    row[column] = int(value)
                if data_type == 'Double':
                    row[column] = float(value)
                if data_type == 'DateTime':
                    row[column] = datetime.strptime(
                        value, "%Y-%m-%dT%H:%M:%SZ")
    return config


def drop_csv_datatype_columns(config):
    for row in config:
        for column in list(row.keys()):
            if column.endswith('@type') or column == 'Timestamp':
                del row[column]
    return config


def filter_records_for_publish(config):
    for i in range(len(config)):
        if 'PUBLISH' in config[i] and config[i]['PUBLISH'] == False:
            del config[i]
    return config


def get_files(folder_path, config_format=None):
    _, _, file_names = next(os.walk(folder_path))
    result = []
    for file_name in file_names:
        if config_format is None or file_name.endswith(config_format):
            file_path = f'{folder_path}/{file_name}'
            result.append(file_path)
    return result


def read_csv_config(file_path):
    with open(file_path, "r") as f:
        reader = csv.DictReader(f)
        config = list(reader)

    cast_csv_datatypes(config)
    drop_csv_datatype_columns(config)
    filter_records_for_publish(config)
    return config


def read_json_config(file_path):
    config = json.load(open(file_path))
    filter_records_for_publish(config)
    return config


def update_configs_from_folder(folder_path, storage_name, storage_key, options):
    config_format = options.get('config_format')
    for file_path in get_files(folder_path, config_format):
        update_config(file_path, storage_name, storage_key)


def update_config(file_path, storage_name, storage_key):
    _, file_extension = os.path.splitext(file_path)
    if file_extension == '.json':
        config = read_json_config(file_path)
    elif file_extension == '.csv':
        config = read_csv_config(file_path)
    else:
        return

    table_name = os.path.splitext(os.path.basename(file_path))[0]
    table.create_table(storage_name, storage_key, table_name)
    table.insert_or_merge(config, storage_name, storage_key, table_name)


def main():

    args = parse_arguments()

    if args.config_path is not None:
        update_config(args.config_path, args.storage_name, args.storage_key)
    else:
        update_configs_from_folder(
            args.config_folder,
            args.storage_name, args.storage_key,
            options={'config_format': args.config_format})


if __name__ == '__main__':
    main()
