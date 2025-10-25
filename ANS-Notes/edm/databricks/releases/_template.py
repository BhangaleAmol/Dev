# Databricks notebook source
# MAGIC %pip install azure-identity==1.12.0
# MAGIC %pip install azure-mgmt-datafactory==3.1.0

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from databricks.functions.shared.azure_data_factory import run_pipeline
from databricks.functions.shared.paths import get_repository_path

parameters = {
    "table_name": [''],
    # "notebooks_path": get_repository_path() + '/databricks/notebooks/' + {path_to_pipeline_folder}
}

pipeline_name = ''
run_pipeline(pipeline_name, parameters)
