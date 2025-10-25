from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import RunFilterParameters
from databricks.functions.shared.config import config
from databricks.functions.shared.secrets import get_secret
from datetime import datetime, timezone, timedelta
import time


def parse_parameters(params: dict):

    params['send_notification'] = params.get('send_notification', False)
    params['send_event'] = params.get('send_event', False)

    if 'table_name' in params:
        params['table_name'] = [t.split('.')[-1] for t in params['table_name']]

    for param_name, param_value in params.items():
        if (type(param_value).__name__ == "str"):
            param_value = param_value.strip()

            if param_value == 'true' or param_value == 'false':
                param_value = param_value == 'true'

        if param_name == 'incremental' and (type(param_value).__name__ == "bool"):
            param_value = 'true' if param_value else 'false'

        params[param_name] = param_value

    print(params)


def pipeline_succeeded(client: DataFactoryManagementClient, run_id: str, config: dict = config) -> bool:

    while True:

        client.pipeline_runs.get(
            config.get('resource_group'),
            config.get('data_factory'),
            run_id)

        filter_params = RunFilterParameters(
            last_updated_after=datetime.now(timezone.utc) - timedelta(1),
            last_updated_before=datetime.now(timezone.utc) + timedelta(1)
        )

        query_response = client.activity_runs.query_by_pipeline_run(
            config.get('resource_group'),
            config.get('data_factory'),
            run_id,
            filter_params
        )

        status = [action.status for action in query_response.value]
        if any(s == "InProgress" for s in status):
            time.sleep(30)
        else:
            if all(s == "Succeeded" for s in status):
                return True
            else:
                return False


def run_pipeline(pipeline_name: str, parameters: dict = {}, wait: bool = False, config: dict = config):
    """
    This function trigges data factory pipeline.
    When wait is true, function will wait for pipeline completion.

    example:\n
    parameters = {\n
        "table_name": ['vw_company'],\n
        "notebooks_path": get_repository_path() + '/databricks/notebooks/bronze/b_col'\n
    }\n
    run_pipeline('col_master_pipeline', parameters)

    requirement:\n
    %pip install azure-identity==1.12.0\n
    %pip install azure-mgmt-datafactory==3.1.0

    """

    credential = ClientSecretCredential(
        config.get('tenant_id'),
        get_secret("svc-edm-dbr-id"),
        get_secret("svc-edm-dbr-secret"))
    client = DataFactoryManagementClient(
        credential, config.get('subscription_id'))

    parse_parameters(parameters)

    response = client.pipelines.create_run(
        config.get('resource_group'),
        config.get('data_factory'),
        pipeline_name,
        parameters=parameters)

    if wait:
        time.sleep(20)
        succeeded = pipeline_succeeded(client, response.run_id, config)
        if not succeeded:
            raise Exception(pipeline_name + " failed.")
        return succeeded
    else:
        return response.run_id
