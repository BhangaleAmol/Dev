# Databricks notebook source
# MAGIC %run ../../FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# INPUT PARAMETERS
database_name = get_input_param('database_name')
file_name = get_input_param('file_name')
handle_delete = get_input_param('handle_delete', 'bool', True)
header = get_input_param('header', 'bool', True)
incremental = get_input_param('incremental', 'bool', False)
incremental_column = get_input_param('incremental_column')
key_columns = get_input_param('key_columns')
overwrite = get_input_param('overwrite', 'bool', False)
partition_column = get_input_param('partition_column')
sampling = get_input_param('sampling', 'bool', False)
source_folder = get_input_param('source_folder')
source_folder_keys = get_input_param('source_folder_keys')
table_name = get_input_param('table_name')
target_container = get_input_param('target_container', 'string', 'datalake')
target_storage = get_input_param('target_storage', 'string', 'edmans{env}data001')
target_folder = get_input_param('target_folder')

# COMMAND ----------

# VALIDATE INPUT
if incremental and key_columns is None:
  raise Exception("INCREMENTAL & NO KEY COLUMNS")

if handle_delete and key_columns is None:
  raise Exception("HANDLE DELETE & NO KEY COLUMNS")
  
if overwrite and sampling:
  raise Exception("OVERWRITE & SAMPLING")
  
# VALIDATE INCREMENTAL WITH OVERWRITE
if incremental and overwrite:
  raise Exception("INCREMENTAL with OVERWRITE not allowed")

# COMMAND ----------

# SETUP VARIABLES
file_path = get_file_path(source_folder, file_name)

if not incremental:
  source_folder_keys = source_folder  
source_file_path_keys = get_file_path(source_folder_keys, file_name)

print('file_name: ' + file_name)
print('source_file_path: ' + file_path)
print('source_file_path_keys: ' + source_file_path_keys)

# COMMAND ----------

# READ DATA
main = (
  spark.read
  .format('csv')
  .option("header", header)
  .option("escape", "\\")
  .option("quote", "\"")
  .option("multiline", "true")
  .load(file_path)
)
main.display()

# COMMAND ----------

# SAMPLING
if sampling:
  main = main.limit(10)

# COMMAND ----------

# BASIC CLEANUP
main_f = (
  main
  .transform(fix_column_names)
  .transform(convert_nan_to_null())
  .transform(trim_all_values)
  .transform_if((partition_column is not None), attach_partition_column(partition_column))
  .transform(attach_modified_date())
  .transform(attach_deleted_flag())
  .distinct()
)

main_f.cache()
main_f.display()

# COMMAND ----------

# VALIDATE DATA
if key_columns is not None:
  check_distinct_count(main_f, key_columns)

# COMMAND ----------

# LOAD
full_name = get_table_name(database_name, table_name)

if (key_columns is None):
  if not sampling:
    options = {'overwrite': True, 'container_name': target_container, 'storage_name': target_storage}
    register_hive_table(main_f, full_name, target_folder, options = options)
    append_into_table(main_f, full_name)
else:
  options = {'overwrite': overwrite, 'container_name': target_container, 'storage_name': target_storage}
  register_hive_table(main_f, full_name, target_folder, options = options)
  merge_into_table(main_f, full_name, key_columns, options = {'auto_merge': True})

# COMMAND ----------

# HANDLE DELETE
if handle_delete and (key_columns is not None) and not sampling:
  
  key_columns_list = key_columns.split(',')  
  full_keys_f = (
    main
    .transform(fix_column_names)
    .transform(convert_nan_to_null())
    .transform(trim_all_values)
    .select(*key_columns_list)    
    .distinct()
    .cache()
  )
  
  apply_soft_delete(full_keys_f, full_name, key_columns)

# COMMAND ----------

# CHECK ROW COUNT
if handle_delete and (key_columns is not None) and not sampling:
  check_row_count(full_name, full_keys_f)

elif not incremental and not sampling:
  check_row_count(full_name, main_f)

# COMMAND ----------

# RETURN MAX VALUE
max_value = get_max_value(main_f, incremental_column)
dbutils.notebook.exit(json.dumps({"max_value": max_value}))
