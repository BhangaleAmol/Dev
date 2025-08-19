# Databricks notebook source
# MAGIC %run ../../FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# INPUT PARAMETERS
database_name = get_input_param('database_name', 'string', 'amazusftp1')
file_name = get_input_param('file_name')
handle_delete = get_input_param('handle_delete', 'bool', True)
header = get_input_param('header', 'bool', True)
key_columns = get_input_param('key_columns')
metadata_container = get_input_param('metadata_container', 'string', 'datalake')
metadata_folder = get_input_param('metadata_folder', 'string', '/datalake/AMAZUSFTP1/metadata')
metadata_storage = get_input_param('metadata_storage', 'string', 'edmans{env}data001')
overwrite = get_input_param('overwrite', 'bool', False)
partition_column = get_input_param('partition_column')
source_folder = get_input_param('source_folder', 'string', '/datalake/AMAZUSFTP1/raw_data/delta_data')
table_name = get_input_param('table_name')
target_container = get_input_param('target_container', 'string', 'datalake')
target_storage = get_input_param('target_storage', 'string', 'edmans{env}data001')
target_folder = get_input_param('target_folder', 'string', '/datalake/AMAZUSFTP1/raw_data/full_data')

# COMMAND ----------

# VALIDATE INPUT
if handle_delete and key_columns is None:
  raise Exception("HANDLE DELETE & NO KEY COLUMNS")

# COMMAND ----------

# VARIABLES
table_name = f'{database_name}.{table_name}'.lower()
print(f'table_name: {table_name}')

source_file_path = get_file_path(source_folder, file_name)
print(f'source_file_path: {source_file_path}')

metadata_file_path = get_file_path(metadata_folder, f'{table_name}.json')
print(f'metadata_file_path: {metadata_file_path}')
