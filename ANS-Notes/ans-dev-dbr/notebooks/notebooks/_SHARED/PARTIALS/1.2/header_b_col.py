# Databricks notebook source
# MAGIC %run ../../FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# INPUT PARAMETERS
database_name = get_json_param('config', 'database_name', 'string', 'col') 
delete_type = get_json_param('config', 'delete_type', "string", "soft_delete")
handle_delete = get_json_param('config', 'handle_delete', "bool", False)
key_columns = get_json_param('config', 'key_columns')
metadata_container = get_json_param('config', 'metadata_container', 'string', 'datalake')
metadata_folder = get_json_param('config', 'metadata_folder', 'string', '/datalake/COL/metadata')
metadata_storage = get_json_param('config', 'metadata_storage', 'string', 'edmans{env}data001')
overwrite = get_json_param('config', 'overwrite', "bool", False)
source_folder = get_json_param('config', 'source_folder')
table_name = get_json_param('config', 'table_name')
target_container = get_json_param('config', 'target_container', 'string', 'datalake')
target_folder = get_json_param('config', 'target_folder', 'string', '/datalake/COL/raw_data/full_data')
target_storage = get_json_param('config', 'target_storage', 'string', 'edmans{env}data001')
test_run = get_json_param('config', 'test_run', "bool", False)

# COMMAND ----------

# VALIDATE INPUT
if handle_delete and key_columns is None:
  raise Exception("HANDLE DELETE & NO KEY COLUMNS")
    
if source_folder is None or table_name is None:
  raise Exception("Source data details are missing")

if target_folder is None or database_name is None or table_name is None:
  raise Exception("Target details are missing")

# COMMAND ----------

# PRINT INPUT
print_input_param('database_name', database_name)
print_input_param('delete_type', delete_type)
print_input_param('handle_delete', handle_delete)
print_input_param('key_columns', key_columns)
print_input_param('metadata_container', metadata_container)
print_input_param('metadata_folder', metadata_folder)
print_input_param('metadata_storage', metadata_storage)
print_input_param('overwrite', overwrite)
print_input_param('source_folder', source_folder)  
print_input_param('table_name', table_name)
print_input_param('target_container', target_container)
print_input_param('target_folder', target_folder)
print_input_param('target_storage', target_storage)
print_input_param('test_run', test_run)

# COMMAND ----------

# SETUP VARIABLES
source_file_path = get_file_path(source_folder, f'{table_name}.par', target_container, target_storage)
print(f'source_file_path: {source_file_path}')

table_name = f'{database_name}.{table_name}'.lower()
print(f'table_name: {table_name}')

metadata_file_path = get_file_path(metadata_folder, f'{table_name}.json', target_container, target_storage)
print(f'metadata_file_path: {metadata_file_path}')

if key_columns is not None:
  key_columns = key_columns.replace(' ', '').split(',')
print(f'key_columns: {key_columns}')
