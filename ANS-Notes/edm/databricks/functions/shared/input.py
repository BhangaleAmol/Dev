from databricks.sdk.runtime import *
from databricks.functions.shared.paths import *
import logging
import json


def cast_param_type(param_value, param_type: str = "string") -> any:
    """
    This function casts values to required data types.
    """

    if param_value == '' or param_value is None:
        return None

    # create list from comma-separated string
    if (type(param_value).__name__ == "str") and param_type == 'list':
        param_value = param_value.split(',')

    param_value = {
        'string': lambda x: str(x),
        'int': lambda x: int(x),
        'bool': lambda x: str(x).lower() in ("yes", "true", "1"),
        'list': lambda x: list(x)
    }[param_type](param_value)
    return param_value


def clean_input_param(param_value, default_value=None) -> any:
    """
    This function cleans input param value.
    """

    if param_value is None or param_value == '':
        param_value = default_value

    if (type(param_value).__name__ == "str"):
        param_value = param_value.strip()

    return param_value


def get_final_input_param(param_value, param_type: str = 'string', default_value=None) -> any:
    """
    This function returns cleaned and typed input param value.
    """

    param_value = clean_input_param(param_value, default_value)
    if param_value is not None:
        param_value = cast_param_type(param_value, param_type)
    return param_value


def get_input_param(param_name: str, param_type: str = 'string', default_value=None):
    """
    This function reads raw input params using dbutils and cleans them.
    """

    dbutils.widgets.text(param_name, "")
    param_value = dbutils.widgets.get(param_name)
    param_value = get_final_input_param(param_value, param_type, default_value)
    return param_value


def get_parameters(config_json: str, params_config: dict = None):
    """
    This function loads json parameters to dict, cleans values and enforces datatypes.
    """

    config = json.loads(config_json)
    params = {}

    if params_config is not None:
        for param_name, param_details in params_config.items():
            if param_name in params_config.keys():
                param_type = param_details.get('field_type', 'string')
                default_value = param_details.get('default_value', None)
                param_value = config.get(param_name)
                params[param_name] = get_final_input_param(
                    param_value, param_type, default_value)
        params['config'] = config
    else:
        params = config

    return params


def print_config(params: dict):
    """
    This function prints config passed to the notebook.
    """

    config = params.pop('config', None)
    if config is not None:
        config = dict(sorted(config.items()))
        print("config = '''\n{")
        for key, value in config.items():
            print(f'\t"{key}": "{value}",')
        print("}\n'''")


def print_parameters(params: dict):
    """
    This function prints all notebook parameters.
    """

    logger = logging.getLogger('edm')
    params = dict(sorted(params.items()))
    for key, value in params.items():
        logger.debug(f'{key}: {value}')
