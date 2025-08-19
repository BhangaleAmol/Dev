# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# INPUT PARAMETERS
database_name = get_input_param('database_name', default_value = 'cti')
key_columns = get_input_param('key_columns', default_value = 'sid')
sampling = get_input_param('sampling', 'bool', False)
source_folder = get_input_param('source_folder', default_value = '/datalake/CTI/raw_data/delta_data_col')
table_name = get_input_param('table_name')
target_folder = get_input_param('target_folder', default_value = '/datalake/CTI/full_raw_data')

# COMMAND ----------

# SETUP VARIABLES
file_name = get_file_name(table_name, 'par')
file_path = get_file_path(source_folder, file_name)

print('file_name: ' + file_name)
print('source_file_path: ' + file_path)

# COMMAND ----------

main = spark.read.format('parquet').load(file_path)

# COMMAND ----------

# SAMPLING
if sampling:
  main = main.limit(10)

# COMMAND ----------

# BASIC CLEANUP
main_f = (
  main
  .transform(fix_column_names)
  .transform(trim_all_values)
  .transform(convert_null_string_to_null(['sid']))
  .dropna(subset=["sid"])
  .distinct()
  .drop('ID')
  .transform(attach_deleted_flag())
  .withColumn('_PART', f.date_format(f.from_unixtime(f.col('cstts') / 1000), "yyyy-MM"))
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
register_hive_table(main_f, full_name, target_folder, options = {'partition_column': "_PART"})
merge_into_table(main_f, full_name, key_columns, options = {'auto_merge': True})

# COMMAND ----------

# RETURN MAX VALUE
max_value = get_cutoff_value_from_current_date()
dbutils.notebook.exit(json.dumps({"max_value": max_value}))

# COMMAND ----------


