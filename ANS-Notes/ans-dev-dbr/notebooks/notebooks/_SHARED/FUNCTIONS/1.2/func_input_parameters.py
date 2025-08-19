# Databricks notebook source
def cast_input_type(value, data_type = "string"):  
  if value == '' or value is None:
    return None
  
  value = {
    'string': lambda x: str(x), 
    'int': lambda x: int(x),
    'bool': lambda x: str(x).lower() in ("yes", "true", "1")
  }[data_type](value)
    
  return value

def get_input_param(field_name, field_type = None, default_value = None):  
  dbutils.widgets.text(field_name, "","") 
  field_value = dbutils.widgets.get(field_name)
  
  if (type(field_value).__name__ == "str"):
    field_value = field_value.strip()  
  
  field_value = None if field_value == '' else field_value 
  
  if field_value is None: 
    field_value = default_value
    
  if field_type is not None:
    field_value = cast_input_type(field_value, field_type)
  
  return field_value

def get_json_param(json_field, field_name, field_type = None, default_value = None):  
  
  json_value = get_input_param(json_field)
   
  if json_value is None:
    field_value = default_value
  else:
    dict_value = json.loads(json_value)  
    field_value = dict_value.get(field_name)
  
  if (type(field_value).__name__ == "str"):
    field_value = field_value.strip()
  
  field_value = None if field_value == '' else field_value 

  if field_value is None: 
    field_value = default_value
    
  if field_type is not None:
    field_value = cast_input_type(field_value, field_type)
  
  return field_value

def get_file_name(schema_name, table_name):
  if schema_name is not None:  
    file_name = schema_name + '.' + table_name
  else:  
    file_name = table_name
  return file_name

def get_table_name(database_name, schema_name, table_name):
  if schema_name is not None:  
    table_name = database_name + '.' + schema_name + '_' + table_name
  else:  
    table_name = database_name + '.' + table_name
  return table_name.lower()

def print_input_param(field_name, field_value):
  field_type = type(field_value).__name__
  if field_type == 'str':
    message = '{0} = "{1}" # [{2}]'.format(field_name, str(field_value), field_type)
  else:
    message = '{0} = {1} # [{2}]'.format(field_name, str(field_value), field_type)
  print(message)

# COMMAND ----------

def print_bronze_csv_params():
  print_input_param('append_only', append_only)
  print_input_param('database_name', database_name)
  print_input_param('handle_delete', handle_delete)
  print_input_param('hard_delete', hard_delete)
  print_input_param('header', header)
  print_input_param('file_name', file_name)
  print_input_param('incremental', incremental)
  print_input_param('incremental_column', incremental_column)
  print_input_param('key_columns', key_columns)
  print_input_param('overwrite', overwrite)
  print_input_param('partition_column', partition_column)
  print_input_param('prune_days', prune_days)
  print_input_param('sampling', sampling)
  print_input_param('source_folder', source_folder)
  print_input_param('source_folder_keys', source_folder_keys)
  print_input_param('table_name', table_name)
  print_input_param('target_container', target_container)
  print_input_param('target_folder', target_folder)
  print_input_param('target_storage', target_storage)
  print_input_param('test_run', test_run)
  print_input_param('escape_char', escape_char)
  
def print_bronze_xlsx_params():
  print_input_param('append_only', append_only)
  print_input_param('database_name', database_name)
  print_input_param('handle_delete', handle_delete)
  print_input_param('header', header)
  print_input_param('file_name', file_name)
  print_input_param('incremental', incremental)
  print_input_param('incremental_column', incremental_column)
  print_input_param('key_columns', key_columns)
  print_input_param('overwrite', overwrite)
  print_input_param('partition_column', partition_column)
  print_input_param('prune_days', prune_days)
  print_input_param('sampling', sampling)
  print_input_param('sheet_name', sheet_name)
  print_input_param('source_folder', source_folder)
  print_input_param('source_folder_keys', source_folder_keys)
  print_input_param('table_name', table_name)
  print_input_param('target_container', target_container)
  print_input_param('target_folder', target_folder)
  print_input_param('target_storage', target_storage)
  print_input_param('test_run', test_run)

def print_bronze_par_params():  
  print_input_param('append_only', append_only)
  print_input_param('database_name', database_name)
  print_input_param('delete_type', delete_type)
  print_input_param('handle_delete', handle_delete)
  print_input_param('incremental', incremental)
  print_input_param('incremental_column', incremental_column)
  print_input_param('incremental_type', incremental_type)
  print_input_param('key_columns', key_columns)
  print_input_param('overwrite', overwrite)
  print_input_param('partition_column', partition_column)
  print_input_param('prune_days', prune_days)
  print_input_param('sampling', sampling)
  print_input_param('schema_name', schema_name)
  print_input_param('source_folder', source_folder)
  print_input_param('source_folder_keys', source_folder_keys)
  print_input_param('table_name', table_name)
  print_input_param('target_container', target_container)
  print_input_param('target_folder', target_folder)
  print_input_param('target_storage', target_storage)
  print_input_param('test_run', test_run)  
  
def print_bronze_ss_params():  
  print_input_param('append_only', append_only)
  print_input_param('database_name', database_name)
  print_input_param('handle_delete', handle_delete)
  print_input_param('incremental', incremental)
  print_input_param('incremental_column', incremental_column)
  print_input_param('key_columns', key_columns)
  print_input_param('overwrite', overwrite)
  print_input_param('partition_column', partition_column)
  print_input_param('prune_days', prune_days)
  print_input_param('sampling', sampling)
  print_input_param('sheet_id', sheet_id)
  print_input_param('source_folder', source_folder)
  print_input_param('table_name', table_name)
  print_input_param('target_container', target_container)
  print_input_param('target_folder', target_folder)
  print_input_param('target_storage', target_storage)
  print_input_param('test_run', test_run) 
  
def print_silver_params():
  print_input_param('append_only', append_only)
  print_input_param('change_path', change_path)
  print_input_param('database_name', database_name)
  print_input_param('delete_type', delete_type)
  print_input_param('handle_delete', handle_delete)
  print_input_param('incremental', incremental) 
  print_input_param('overwrite', overwrite)
  print_input_param('partition_column', partition_column)
  print_input_param('prune_days', prune_days)
  print_input_param('run_datetime', run_datetime)  
  print_input_param('sampling', sampling)
  print_input_param('table_name', table_name)
  print_input_param('target_container', target_container)
  print_input_param('target_folder', target_folder)
  print_input_param('target_storage', target_storage)
  print_input_param('test_run', test_run)
