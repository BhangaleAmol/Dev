from databricks.functions.shared.paths import *
from datetime import datetime


def attach_variables(params: dict):
    """
    This function attaches to parameters variables calcualted during run.
    """

    params['partition_columns'] = ['_DELETED', '_DATE']

    params['s_table_name'] = params.get('table_name')

    params['target_endpoint'] = get_endpoint_path(
        params.get('target_container'),
        params.get('target_storage'))

    params['table_name'] = f"{params.get('database_name')}.{params.get('table_name')}"
    params['table_name'] = params.get('table_name').lower()

    params['table_path'] = get_table_path(
        params.get('table_name'),
        params.get('target_endpoint')
    ).replace('.dlt', '.par')

    params['s_table_name'] = params.get('s_table_name').lower()

    params['schema_endpoint'] = get_endpoint_path(
        params.get('metadata_container'),
        params.get('metadata_storage'))

    params['schema_path'] = get_file_path(
        f"{params.get('metadata_folder')}/{params.get('table_name')}.json",
        params.get('schema_endpoint')
    )

    params['run_datetime'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

    return params


def get_params_config() -> dict:
    return dict(
        append_only={'field_type': 'bool', 'default_value': False},
        database_name={'default_value': 's_core'},
        handle_delete={'field_type': 'bool', 'default_value': False},
        incremental={'field_type': 'bool', 'default_value': False},
        metadata_container={'default_value': 'datalake'},
        metadata_folder={'default_value': '/datalake/s_core/metadata'},
        metadata_storage={'default_value': 'edmans{env}data002'},
        overwrite={'field_type': 'bool', 'default_value': False},
        prune_days={'field_type': 'int', 'default_value': 30},
        table_name={},
        target_container={'default_value': 'datalake'},
        target_folder={'default_value': '/datalake/s_core/full_data'},
        target_storage={'default_value': 'edmans{env}data002'},
    )


def validate_parameters(params: dict):
    if params.get('incremental') and params.get('overwrite'):
        raise Exception('incremental & overwrite')
