# Databricks notebook source
# INPUT PARAMETERS
database_name = get_json_param('config', 'database_name') 
incremental = get_json_param('config', 'incremental', "bool", False)
overwrite = get_json_param('config', 'overwrite', "bool", False)
partition_columns = get_json_param('config', 'partition_columns')
source_folder = get_json_param('config', 'source_folder')
table_name = get_json_param('config', 'table_name')
target_container = get_json_param('config', 'target_container', 'string', 'datalake')
target_folder = get_json_param('config', 'target_folder')
target_storage = get_json_param('config', 'target_storage', 'string', 'edmans{env}data001')
test_run = get_json_param('config', 'test_run', "bool", False)

# COMMAND ----------

# INPUT CLEANUP
if partition_columns is not None and isinstance(partition_columns, str):
  partition_columns = [c.strip() for c in partition_columns.replace(' ', '').split(',')]

# COMMAND ----------

print_input_param('database_name', database_name)
print_input_param('incremental', incremental)
print_input_param('overwrite', overwrite)
print_input_param('partition_columns', partition_columns)
print_input_param('source_folder', source_folder)
print_input_param('table_name', table_name)
print_input_param('target_container', target_container)
print_input_param('target_folder', target_folder)
print_input_param('target_storage', target_storage)
print_input_param('test_run', test_run)  
