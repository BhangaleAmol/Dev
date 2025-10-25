from databricks.functions.shared.input import get_input_param, get_parameters, print_parameters
from databricks.functions.b_col.input import get_params_config, attach_variables


def header(params):

    config = get_input_param('config')
    if config is None:
        config = params.get('config')

    config_params = get_parameters(config, get_params_config())
    params = {**params, **config_params}
    params = attach_variables(params)
    print_parameters(params)
    return params
