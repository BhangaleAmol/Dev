# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# INPUT PARAMETERS
database_name = get_input_param('database_name')
incremental = get_input_param('incremental', 'bool', False)
incremental_column = get_input_param('incremental_column')
key_columns = get_input_param('key_columns')
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
if incremental and (key_columns is None):
  raise Exception("INCREMENTAL & NO KEY COLUMNS")

if overwrite and incremental:
  raise Exception("OVERWRITE & INCREMENTAL")

if overwrite and sampling:
  raise Exception("OVERWRITE & SAMPLING")

# COMMAND ----------

# SETUP VARIABLES

file_name = get_file_name(table_name, schema_name = schema_name, file_extension = 'par')
file_path = get_file_path(source_folder, file_name)

print('file_name: ' + file_name)
print('file_path: ' + file_path)

# COMMAND ----------

# READ
main_f = (
  spark.read.format('parquet').load(file_path)
  .transform(attach_modified_date())
  .transform(attach_partition_column('_MODIFIED'))
)

# COMMAND ----------

# SAMPLING
if sampling:
  main_f = main_f.limit(10)

# COMMAND ----------

# LOAD
full_name = get_table_name(database_name, table_name)

# full overwrite
if key_columns is None:
  if not sampling:
    options = {'overwrite': True, 'configure_table': False}
    register_hive_table(main_f, full_name, target_folder, options = options)
    append_into_table(main_f, full_name)

# merge with keys
else:
  options = {'overwrite': overwrite, 'configure_table': False}
  register_hive_table(main_f, full_name, target_folder, options = options)
  merge_into_table(main_f, full_name, key_columns, options = {'auto_merge': True})

# COMMAND ----------

# RETURN MAX VALUE
max_value = get_max_value(main_f, incremental_column)
dbutils.notebook.exit(json.dumps({"max_value": max_value}))

# COMMAND ----------


