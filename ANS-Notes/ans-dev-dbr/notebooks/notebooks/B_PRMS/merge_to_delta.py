# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# INPUT PARAMETERS
append_only = get_input_param('append_only', 'bool', False)
database_name = get_input_param('database_name')
handle_delete = get_input_param('handle_delete', 'bool', True)
hard_delete = get_input_param('hard_delete', 'bool', False)
incremental = get_input_param('incremental', 'bool', False)
incremental_column = get_input_param('incremental_column')
incremental_type = get_input_param('incremental_type', 'string', 'timestamp')
key_columns = get_input_param('key_columns')
overwrite = get_input_param('overwrite', 'bool', False)
partition_column = get_input_param('partition_column')
prune_days = get_input_param('prune_days', 'int', 30)
sampling = get_input_param('sampling', 'bool', False)
schema_name = get_input_param('schema_name')
source_folder = get_input_param('source_folder')
source_folder_keys = get_input_param('source_folder_keys')
table_name = get_input_param('table_name')
target_folder = get_input_param('target_folder')
test_run = get_input_param('test_run', 'bool', False)
file_extension =  get_input_param('file_extension', 'string', 'par')

# COMMAND ----------

# VALIDATE INPUT
if incremental and (key_columns is None) and append_only is False:
  raise Exception("INCREMENTAL & NO KEY COLUMNS")

if overwrite and incremental:
  raise Exception("OVERWRITE & INCREMENTAL")

if overwrite and sampling:
  raise Exception("OVERWRITE & SAMPLING")

# COMMAND ----------

# SETUP VARIABLES
file_name = get_file_name(table_name, schema_name = schema_name, file_extension = 'par')
file_path = get_file_path(source_folder, file_name)

if handle_delete and not incremental:
  source_folder_keys = source_folder
  file_path_keys = get_file_path(source_folder_keys, file_name)
  print(f'file_path_keys: {file_path_keys}')
  

full_table_name = get_table_name(database_name, table_name, schema_name)

if key_columns is not None:
  key_columns = key_columns.replace(' ', '').split(',')
  if not isinstance(key_columns, list):
    key_columns = [key_columns]

print(f'file_name: {file_name}')
print(f'file_path: {file_path}')
print(f'full_table_name: {full_table_name}')

# COMMAND ----------

# READ
main = spark.read.format('parquet').load(file_path)

# COMMAND ----------

# SAMPLING
if sampling:
  main = main.limit(10)

# COMMAND ----------

# TRANSFORM
main_2 = (
  main
  .transform(fix_column_names)
  .transform(trim_all_values)
  .transform_if((partition_column is not None), attach_partition_column(partition_column))
  .transform(attach_modified_date())
  .transform(attach_deleted_flag())
  .distinct()
)

# COMMAND ----------

# DUPLICATES NOTIFICATION
if key_columns is not None:
  duplicates = get_duplicate_rows(main_2, key_columns)
  duplicates.cache()

  if not duplicates.rdd.isEmpty():
    notebook_data = {
      'source_name': 'PRMS',
      'notebook_name': NOTEBOOK_NAME,
      'notebook_path': NOTEBOOK_PATH,
      'target_name': full_table_name,
      'duplicates_count': duplicates.count(),
      'duplicates_sample': duplicates.select(key_columns).limit(50)
    }
    send_mail_duplicate_records_found(notebook_data)
  
  duplicates.unpersist()

# COMMAND ----------

# DROP DUPLICATES
if key_columns is not None:
  main_f = main_2.dropDuplicates(key_columns)
else:
  main_f = main_2
  
main_f.display()

# COMMAND ----------

# LOAD

# append only
if append_only:
  if not sampling:
    register_hive_table(main_f, full_table_name, target_folder, options = {'overwrite': overwrite,'file_extension':file_extension})
    append_into_table(main_f, full_table_name, options = {'incremental_column': incremental_column})

# full overwrite
elif key_columns is None:
  if not sampling:
    register_hive_table(main_f, full_table_name, target_folder, options = {'overwrite': True,'file_extension':file_extension})
    append_into_table(main_f, full_table_name)

# merge with keys
else:
  register_hive_table(main_f, full_table_name, target_folder, options = {'overwrite': overwrite,'file_extension':file_extension})
  merge_into_table(main_f, full_table_name, key_columns, options = {'auto_merge': True})

# COMMAND ----------

# HANDLE DELETE
if handle_delete and (key_columns is not None) and not sampling:
  
  full_keys_f = (
    spark.read.format('parquet').load(file_path_keys)
    .transform(fix_column_names)
    .transform(trim_all_values)
    .select(*key_columns)    
    .distinct()  
  )
  
  if not hard_delete:
    apply_soft_delete(full_keys_f, full_table_name, key_columns)
    
  if hard_delete:
    apply_hard_delete(full_keys_f, full_table_name, key_columns)

# COMMAND ----------

# RETURN MAX VALUE
max_value = get_max_value(main_f, incremental_column)
dbutils.notebook.exit(json.dumps({"max_value": max_value}))
