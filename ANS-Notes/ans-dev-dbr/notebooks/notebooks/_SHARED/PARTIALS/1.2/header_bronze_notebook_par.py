# Databricks notebook source
# INPUT PARAMETERS
append_only = get_json_param('config', 'append_only', "bool", False)
database_name = get_json_param('config', 'database_name') 
delete_type = get_json_param('config', 'delete_type', "string", "soft_delete")
handle_delete = get_json_param('config', 'handle_delete', "bool", False)
incremental = get_json_param('config', 'incremental', "bool", False)
incremental_column = get_json_param('config', 'incremental_column')
incremental_type = get_json_param('config', 'incremental_type', 'string', 'timestamp')
key_columns = get_json_param('config', 'key_columns')
overwrite = get_json_param('config', 'overwrite', "bool", False)
partition_columns = get_json_param('config', 'partition_columns')
partition_column = get_json_param('config', 'partition_column')
prune_days = get_json_param('config', 'prune_days', "int", 30)
sampling = get_json_param('config', 'sampling', "bool", False) 
schema_name = get_json_param('config', 'schema_name')
source_folder = get_json_param('config', 'source_folder')
source_folder_keys = get_json_param('config', 'source_folder_keys')
table_name = get_json_param('config', 'table_name')
target_container = get_json_param('config', 'target_container', 'string', 'datalake')
target_folder = get_json_param('config', 'target_folder')
target_storage = get_json_param('config', 'target_storage', 'string', 'edmans{env}data001')
test_run = get_json_param('config', 'test_run', "bool", False)

# COMMAND ----------

# INPUT CLEANUP
if key_columns is not None:
  key_columns = key_columns.replace(' ', '')
  
if partition_columns is not None and isinstance(partition_columns, str):
  partition_columns = [c.strip() for c in partition_columns.replace(' ', '').split(',')]

# COMMAND ----------

if sampling:
  handle_delete = False
  overwrite = False
