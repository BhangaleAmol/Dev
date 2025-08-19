# Databricks notebook source
# EXTRACT INPUT PARAMETERS
dbutils.widgets.removeAll()
input_params = get_input_params(['source_folder', 'target_folder', 'database_name', 'sampling', 'test_run'])
item = get_input_param_json('item')
params = {**input_params, **item}
format_default_params(params)

# COMMAND ----------

print(params)
