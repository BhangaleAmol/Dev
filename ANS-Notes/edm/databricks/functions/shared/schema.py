from databricks.functions.shared.dataframe import DataFrame
import json
import imp
from databricks.functions.shared.catalog import get_table_name, split_table_name, split_database_name, get_table_schema, get_table_location
from databricks.functions.shared.paths import get_schema_folder_path
from databricks.functions.shared.storage import list_files
from pyspark.sql.types import *


def get_schema_as_json(table_name: str) -> dict:
    """
    This function returns json containing table schema.
    """

    table_name = get_table_name(table_name)

    schema_dict = get_table_schema(table_name)
    table_path = get_table_location(table_name)
    folder_path = table_path.split('core.windows.net')[1]

    data_source_no = table_path.split('@')[1].split('.')[0][-3:]
    data_source = f'datalake{data_source_no}'

    return json.dumps({
        'schema': schema_dict,
        'table_path': table_path,
        'data_source': data_source,
        'folder_path': folder_path
    })


def get_schema_as_dict(df: DataFrame) -> dict:
    """
    This function returns dict schema from dataframe.
    """

    result = {}
    for data_type in df.dtypes:
        result[data_type[0]] = data_type[1]
    return result


def get_dict_schema_from_config(table_name: str) -> dict:
    """
    This function returns dict schema from config file.
    """

    _, database_name, s_table_name = split_table_name(table_name)
    file_path = get_schema_folder_path() + '/' + database_name + \
        '/' + s_table_name + '.py'
    file_path = file_path.replace('file:/', '/')
    module = imp.load_source(s_table_name, file_path)
    return module.get_schema()


def get_schema_from_config(table_name: str) -> StructType:
    """
    This function returns StructType schema from dict schema file.
    """

    table_schema = get_dict_schema_from_config(table_name)
    fields = []
    for field_name, field_type in table_schema.items():
        field_type = field_type if field_type != 'int' else 'integer'
        field = {
            'metadata': {},
            'name': field_name,
            'nullable': True,
            'type': field_type
        }
        fields.append(field)

    dict_schema = {
        'fields': fields,
        'type': 'struct'
    }

    return StructType.fromJson(dict_schema)


def get_tables_for_schema_files(database_name: str = None) -> list:
    """
    This function returns list of all table names from folder containing dict schemas.
    Doesn't work on shared cluster anymore.
    """

    folder_path = get_schema_folder_path()
    if database_name is not None:
        _, database_name = split_database_name(database_name)
        folder_path += '/' + database_name

    file_list = list_files(f'file:{folder_path}')
    return [database_name + '.' + f.name.replace('.py', '') for f in file_list]
