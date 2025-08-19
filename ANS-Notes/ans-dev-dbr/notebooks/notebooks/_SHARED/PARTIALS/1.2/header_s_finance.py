# Databricks notebook source
# DEFAULT PARAMETERS
def_source_name = 'DEFAULT' if len(NOTEBOOK_NAME.split('_')) == 1 else NOTEBOOK_NAME.split('_')[-1].upper()
def_table_name = NOTEBOOK_NAME.lower()

# COMMAND ----------

# INPUT PARAMETERS
append_only = get_json_param('config', 'append_only', "bool", False)
change_path = get_json_param('config', 'change_path', "bool", False)
database_name = get_json_param('config', 'database_name', "string", 's_finance') 
delete_type = get_json_param('config', 'delete_type', "string", "soft_delete")
handle_delete = get_json_param('config', 'handle_delete', "bool", False)
incremental = get_json_param('config', 'incremental', "bool", False)
metadata_container = get_json_param('config', 'metadata_container', 'string', 'datalake')
metadata_folder = get_json_param('config', 'metadata_folder', 'string', '/datalake/s_finance/metadata')
metadata_storage = get_json_param('config', 'metadata_storage', 'string', 'edmans{env}data002')
overwrite = get_json_param('config', 'overwrite', "bool", False)
partition_column = get_json_param('config', 'partition_column')
prune_days = get_json_param('config', 'prune_days', "int", 30)
run_datetime = dt.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
sampling = get_json_param('config', 'sampling', "bool", False)
source_name = get_json_param('config', 'source_name', 'string', def_source_name)
table_name = get_json_param('config', 'table_name', 'string', def_table_name)
target_container = get_json_param('config', 'target_container', 'string', 'datalake')
target_folder = get_json_param('config', 'target_folder', 'string', '/datalake/s_finance/full_data')
target_storage = get_json_param('config', 'target_storage', 'string', 'edmans{env}data002')
temp_folder = get_json_param('config', 'temp_folder', 'string', '/datalake/s_finance/temp_data')
test_run = get_json_param('config', 'test_run', "bool", False)

# COMMAND ----------

# VARIABLES
table_name = f'{database_name}.{table_name}'.lower()

# COMMAND ----------

# PRINT INPUT
print_input_param('append_only', append_only)
print_input_param('change_path', change_path)
print_input_param('database_name', database_name)
print_input_param('delete_type', delete_type)
print_input_param('handle_delete', handle_delete)
print_input_param('incremental', incremental) 
print_input_param('metadata_container', metadata_container)
print_input_param('metadata_folder', metadata_folder)
print_input_param('metadata_storage', metadata_storage)
print_input_param('overwrite', overwrite)
print_input_param('partition_column', partition_column)
print_input_param('prune_days', prune_days)
print_input_param('run_datetime', run_datetime)  
print_input_param('sampling', sampling)
print_input_param('source_name', source_name)
print_input_param('table_name', table_name)
print_input_param('target_container', target_container)
print_input_param('target_folder', target_folder)
print_input_param('target_storage', target_storage)
print_input_param('temp_folder', temp_folder)
print_input_param('test_run', test_run)

# COMMAND ----------

#  VALIDATE INPUT
if incremental and overwrite:
	raise Exception("incremental & overwrite")

if sampling and overwrite:
	raise Exception("sampling & overwrite")
