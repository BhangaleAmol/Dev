from databricks.functions.shared.input import get_input_param, get_parameters, print_parameters
from databricks.functions.s_core.input import get_params_config, attach_variables, validate_parameters


def header(params):

    config = get_input_param('config')
    if config is None:
        config = params.get('config')

    config_params = get_parameters(config, get_params_config())
    params = {**params, **config_params}
    validate_parameters(params)
    params = attach_variables(params)
    print_parameters(params)
    return params
