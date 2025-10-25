import os
from databricks.sdk.runtime import *

ENV_NAME = os.getenv('ENV_NAME')

NOTEBOOK_PATH = dbutils.notebook.entry_point.getDbutils(
).notebook().getContext().notebookPath().get()
NOTEBOOK_NAME = os.path.basename(NOTEBOOK_PATH)

config = {
    'catalog_name': f'edm_{ENV_NAME}',
    'cfg_storage_name': f'edmans{ENV_NAME}config001',
    'cfg_storage_secret_name': 'ls-azu-config-table-sas-key',
    'data_factory': f'edm-ans-{ENV_NAME}-df',
    'env_name': ENV_NAME,
    'notebook_name': NOTEBOOK_NAME,
    'notebook_path': NOTEBOOK_PATH,
    'resource_group': f'edm-ans-{ENV_NAME}-rg',
    'scope_name': 'edm-scope',
    'subscription_id': 'a64abb5d-8b1a-45bf-9eca-7096de5d4fac',
    'timestamp_table_name': 'edmtimestamps',
    'tenant_id': 'e49ea3fe-87f8-44df-a7ba-2131f4fb91f1',
}
