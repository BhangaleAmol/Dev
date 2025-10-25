from databricks.functions.shared.config import config
from databricks.functions.shared.catalog import split_table_name
from databricks.sdk.runtime import *


def get_endpoint_path(
        container_name: str = 'datalake',
        storage_name: str = 'edmans{env}data001', config=config) -> str:
    """
    This function returns abfss path in the form of abfss://{container_name}@{storage_name}.dfs.core.windows.net
    """

    env_name = config.get('env_name')
    storage_name = storage_name.replace('{env}', env_name)
    return f'abfss://{container_name}@{storage_name}.dfs.core.windows.net'


def get_file_path(file_path: str, endpoint_path: str) -> str:
    """
    This function returns full abfss path to file.
    """

    if file_path[0] == '/':
        file_path = file_path[1:]
    return f'{endpoint_path}/{file_path}'


def get_folder_path(folder_path: str, endpoint_path: str) -> str:
    """
    This function returns full abfss path to folder.
    """

    if folder_path[0] == '/':
        folder_path = folder_path[1:]
    return f'{endpoint_path}/{folder_path}'


def get_repository_path() -> str:
    """
    This function returns workspace path (/Workspace/...) to repository root.
    """

    notebook_path = dbutils.notebook.entry_point.getDbutils(
    ).notebook().getContext().notebookPath().get()
    return '/Workspace/' + '/'.join(notebook_path.split('/')[1:4])


def get_schema_folder_path() -> str:
    """
    This function returns workspace path to folder containing schema dicts.
    """

    repository_path = get_repository_path()
    return f"{repository_path}/databricks/schema"


def get_table_path(table_name: str, endpoint_path: str) -> str:
    """
    This function returns full abfss path to table.
    """

    _, database_name, s_table_name = split_table_name(table_name)
    s_table_name = s_table_name.lower()
    file_path = f'datalake/{database_name}/raw_data/full_data/{s_table_name}.dlt'
    return get_file_path(file_path, endpoint_path)


def get_test_data_folder_path() -> str:
    """
    This function returns workspace path to folder containing test data.
    """

    repository_path = get_repository_path()
    return f"{repository_path}/tests/notebooks/test_data"


def get_test_database_path(
        database_name: str, container_name: str = 'datalake',
        storage_name: str = 'edmans{env}data001') -> str:
    """
    This function returns full abfss path to test database.
    """

    endpoint_path = get_endpoint_path(
        container_name, storage_name, config=config)
    folder_path = f'tests/{database_name}'
    return get_folder_path(folder_path, endpoint_path)


def get_test_table_path(
        table_name: str, container_name: str = 'datalake',
        storage_name: str = 'edmans{env}data001') -> str:
    """
    This function returns full abfss path to test table.
    """

    _, database_name, s_table_name = split_table_name(table_name)
    database_path = get_test_database_path(
        database_name, container_name, storage_name)
    return f'{database_path}/{s_table_name}.dlt'
