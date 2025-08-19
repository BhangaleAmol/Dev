# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

spark.conf.set('spark.sql.autoBroadcastJoinThreshold', -1)

# COMMAND ----------

# INPUT PARAMETERS
append_only = get_input_param('append_only', 'bool', False)
database_name = get_input_param('database_name')
handle_delete = get_input_param('handle_delete', 'bool', True)
hard_delete = get_input_param('hard_delete', 'bool', False)
incremental = get_input_param('incremental', 'bool', False)
incremental_column = get_input_param('incremental_column')
key_columns = get_input_param('key_columns')
metadata_container = get_input_param('metadata_container', 'string', 'datalake')
metadata_folder = get_input_param('metadata_folder', 'string', '/datalake/EBS/metadata')
metadata_storage = get_input_param('metadata_storage', 'string', 'edmans{env}data001')
overwrite = get_input_param('overwrite', 'bool', False)
partition_column = get_input_param('partition_column')
sampling = get_input_param('sampling', 'bool', False)
schema_name = get_input_param('schema_name')
source_folder = get_input_param('source_folder')
source_folder_keys = get_input_param('source_folder_keys')
table_name = get_input_param('table_name')
target_folder = get_input_param('target_folder')

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

f_table_name = get_table_name(database_name, table_name)

print('file_name: ' + file_name)
print('file_path: ' + file_path)
print('file_path_keys: ' + file_path_keys)
print('f_table_name: ' + f_table_name)

# COMMAND ----------

# READ
main = spark.read.format('parquet').load(file_path)

# COMMAND ----------

# SAMPLING
if sampling:
  main = main.limit(10)

# COMMAND ----------

# TRANSFORM
main_f = (
  main
  .transform(fix_column_names)
  .transform(trim_all_values)
  .transform_if((partition_column is not None), attach_partition_column(partition_column))
  .transform(attach_modified_date())
  .transform(attach_deleted_flag())
  .distinct()
)

main_f.cache()

# COMMAND ----------

# VALIDATE DATA
if (key_columns is not None) and incremental:  
  check_distinct_count(main_f, key_columns)

# COMMAND ----------

# LOAD

# append only
if append_only:
  if not sampling:
    register_hive_table(main_f, f_table_name, target_folder, options = {'overwrite': overwrite})
    append_into_table(main_f, f_table_name, options = {'incremental_column': incremental_column})

# full overwrite
elif key_columns is None:
  if not sampling:
    register_hive_table(main_f, f_table_name, target_folder, options = {'overwrite': True})
    append_into_table(main_f, f_table_name)

# merge with keys
else:
  register_hive_table(main_f, f_table_name, target_folder, options = {'overwrite': overwrite})
  merge_into_table(main_f, f_table_name, key_columns, options = {'auto_merge': True})

# COMMAND ----------

# HANDLE DELETE
if handle_delete and (key_columns is not None) and not sampling:  
  
  key_columns_list = key_columns.replace(' ', '').split(',')
  full_keys_f = (
    spark.read.format('parquet').load(file_path_keys)
    .transform(fix_column_names)
    .transform(trim_all_values)
    .select(*key_columns_list)    
    .distinct()
    .cache()
  )
  
  if not hard_delete:
    apply_soft_delete(full_keys_f, f_table_name, key_columns)
    
  if hard_delete:
    apply_hard_delete(full_keys_f, f_table_name, key_columns)

# COMMAND ----------

# CHECK ROW COUNT
if handle_delete and (key_columns is not None) and not sampling:
  check_row_count(f_table_name, full_keys_f)

elif not incremental and not sampling:
  check_row_count(f_table_name, main_f)

# COMMAND ----------

# EXPORT TABLE META
schema_dict = get_table_schema(f_table_name)
table_path = get_table_location(f_table_name)
folder_path = table_path.split('core.windows.net')[1]

data_source_no = table_path.split('@')[1].split('.')[0][-3:]
data_source = f'datalake{data_source_no}'

result = json.dumps({
  'schema': schema_dict,
  'table_path': table_path,
  'data_source': data_source,
  'folder_path': folder_path
})

endpoint_name = get_endpoint_name(metadata_container, metadata_storage)
file_name = f'{f_table_name}.json'.lower()
file_path = f'{endpoint_name}{metadata_folder}/{file_name}'
dbutils.fs.put(file_path, result, True)
print(f'metadata saved to: {file_path}')

# COMMAND ----------

# RETURN MAX VALUE
max_value = get_max_value(main_f, incremental_column)
dbutils.notebook.exit(json.dumps({"max_value": max_value}))

# COMMAND ----------


