# Databricks notebook source
# INPUT
database_name = get_input_param('database_name')
incremental = get_input_param('incremental', 'bool', default_value = False)
metadata_container = get_input_param('metadata_container', default_value = 'datalake')
metadata_folder = get_input_param('metadata_folder')
metadata_storage = get_input_param('metadata_storage', default_value = 'edmans{env}data003')
overwrite = get_input_param('overwrite', 'bool', default_value = False)
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name')
target_container = get_input_param('target_container', default_value = 'datalake')
target_folder = get_input_param('target_folder')
target_storage = get_input_param('target_storage', default_value = 'edmans{env}data003')

# COMMAND ----------

# VALIDATE INPUT
if overwrite and sampling:
  raise Exception("OVERWRITE & SAMPLING")
