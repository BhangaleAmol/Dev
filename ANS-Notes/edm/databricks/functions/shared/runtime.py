from pyspark.sql import SparkSession
from databricks.sdk.runtime import *
import json


def get_current_user() -> str:
    """
    This function returns name of user executing code.
    """

    spark = SparkSession.builder.getOrCreate()
    return (
        spark.sql('SELECT CURRENT_USER() as user')
        .collect()[0]['user']
    )


def is_adf_run() -> bool:
    """
    This function checks if current user if adf service account to detect 
    if this is automated run triggered by the data factory. 
    """

    return get_current_user() == 'svc-edm-adf@ansell.com'


def run_notebook(notebook_path: str, config: dict, timeout: int = 0):
    """
    This function runs notebook.

    example:\n
    config = {"table_name": "account_col"}\n
    notebook_path = "../notebooks/silver/s_core/account_col"
    """

    return dbutils.notebook.run(notebook_path, timeout, {"config": f"{json.dumps(config)}"})
