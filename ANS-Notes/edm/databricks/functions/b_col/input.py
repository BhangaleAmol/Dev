from databricks.functions.shared.paths import *


def attach_variables(params: dict):
    """
    This function attaches to parameters variables calcualted during run.
    """

    params['partition_columns'] = ['_DELETED', '_DATE']

    params['s_table_name'] = params.get('table_name')

    params['source_endpoint'] = get_endpoint_path(
        params.get('source_container'),
        params.get('source_storage'))

    params['target_endpoint'] = get_endpoint_path(
        params.get('target_container'),
        params.get('target_storage'))

    params['source_file'] = get_file_path(
        f"{params.get('source_folder')}/{params.get('s_table_name')}.par",
        params.get('source_endpoint'))

    params['table_name'] = f"{params.get('database_name')}.{params.get('table_name')}"
    params['table_name'] = params.get('table_name').lower()

    params['table_path'] = get_table_path(
        params.get('table_name'),
        params.get('target_endpoint')
    ).replace('col', 'COL').replace('.dlt', '.par')

    params['s_table_name'] = params.get('s_table_name').lower()

    params['schema_endpoint'] = get_endpoint_path(
        params.get('metadata_container'),
        params.get('metadata_storage'))

    params['schema_path'] = get_file_path(
        f"{params.get('metadata_folder')}/{params.get('table_name')}.json",
        params.get('schema_endpoint')
    ).replace('/col/', '/COL/')

    return params


def get_params_config():
    return dict(
        append_only={'field_type': 'bool', 'default_value': False},
        database_name={'default_value': 'col'},
        handle_delete={'field_type': 'bool', 'default_value': False},
        metadata_container={'default_value': 'datalake'},
        metadata_folder={'default_value': '/datalake/COL/metadata'},
        metadata_storage={'default_value': 'edmans{env}data001'},
        overwrite={'field_type': 'bool', 'default_value': False},
        source_folder={'default_value': '/datalake/COL/raw_data/delta_data'},
        source_container={'default_value': 'datalake'},
        source_storage={'default_value': 'edmans{env}data001'},
        table_name={},
        target_container={'default_value': 'datalake'},
        target_folder={'default_value': '/datalake/COL/raw_data/full_data'},
        target_storage={'default_value': 'edmans{env}data001'},
    )
