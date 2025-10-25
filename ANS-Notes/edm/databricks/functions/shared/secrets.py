from databricks.sdk.runtime import *
from databricks.functions.shared.config import config


def get_secret(secret_name, config=config):
    return dbutils.secrets.get(scope=config.get('scope_name'), key=secret_name)
