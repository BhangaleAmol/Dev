# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_bronze_notebook_par

# COMMAND ----------

# INPUT PARAMETERS
print_bronze_par_params()

# COMMAND ----------

# VALIDATE SOURCE DETAILS
if source_folder is None or table_name is None:
  raise Exception("Source data details are missing")

if handle_delete and source_folder_keys is None and incremental is True:
  raise Exception("Full keys folder is missing")
  
# VALIDATE TARGET DETAILS
if target_folder is None or database_name is None or table_name is None:
  raise Exception("Target details are missing")

# VALIDATE INCREMENTAL
if incremental and key_columns is None and append_only is False:
  raise Exception("INCREMENTAL operation not possible")
  
# VALIDATE HANDLE DELETE
if handle_delete and (key_columns is None or append_only is True):
  raise Exception("HANDLE DELETE operation not possible")
  
# VALIDATE INCREMENTAL AND OVERWRITE
if incremental and overwrite:
  raise Exception("INCREMENTAL with OVERWRITE not allowed")

# COMMAND ----------

key_columns_list = key_columns
if key_columns is not None:
  key_columns = key_columns.replace(' ', '').split(',')
  if not isinstance(key_columns, list):
    key_columns = [key_columns]

# COMMAND ----------

# SETUP VARIABLES
endpoint_name = get_endpoint_name(target_container, target_storage)
file_name = get_file_name(schema_name, table_name)
table_name = get_table_name(database_name, schema_name, table_name)
source_file_path = '{0}{1}/{2}.par'.format(endpoint_name, source_folder, file_name)

print('table_name: ' + table_name)
print('file_name: ' + file_name)
print('source_file_path: ' + source_file_path)

if handle_delete:
  if not incremental:
    source_folder_keys = source_folder
  source_file_path_keys = '{0}{1}/{2}.par'.format(endpoint_name, source_folder_keys, file_name)
  print('source_file_path_keys: ' + source_file_path_keys)

# COMMAND ----------

# READ DATA
main = spark.read.format('parquet').load(source_file_path)
display(main)

# COMMAND ----------

# SAMPLING
if sampling:
  main = main.limit(10)

# COMMAND ----------

# BASIC CLEANUP
main2 = (
  main
  .transform(fix_column_names)
  .transform(trim_all_values)
  .distinct()
)
main2.display()

# COMMAND ----------

# PARTITION DATA
if partition_column is not None:
  main3 = main2.transform(attach_partition_column(partition_column))
  partition_field = '_PART'
else:
  main3 = main2
  partition_field = None

# COMMAND ----------

main_4 = (
  main3
  .withColumn('_DELETED', f.lit(False))
  .withColumn('_MODIFIED', f.current_timestamp())
)

# COMMAND ----------

# SEND DUPLICATES NOTIFICATION
if key_columns is not None:
  duplicates = get_duplicate_rows(main_4, key_columns)
  duplicates.cache()

  if not duplicates.rdd.isEmpty():
    notebook_data = {
      'source_name': 'TOT',
      'notebook_name': NOTEBOOK_NAME,
      'notebook_path': NOTEBOOK_PATH,
      'target_name': file_name,
      'duplicates_count': duplicates.count(),
      'duplicates_sample': duplicates.select(key_columns).limit(50)
    }
    send_mail_duplicate_records_found(notebook_data)

  duplicates.unpersist()

# COMMAND ----------

# DROP DUPLICATES  
if key_columns is not None:
  main_f = main_4.dropDuplicates(key_columns)
else:
  main_f = main_4
  
main_f.cache()
main_f.display()

# COMMAND ----------

options = {
  'append_only': append_only,
  'incremental': incremental,
  'incremental_column': incremental_column,
  'overwrite': overwrite,
  'partition_column': partition_column,
  'sampling': sampling,
  'target_container': target_container,
  'target_storage': target_storage
}

merge_raw_to_delta(main_f, table_name, key_columns, target_folder, options)

# COMMAND ----------

# HANDLE DELETE
if handle_delete and (key_columns is not None) and not sampling:
  
  full_keys_f = (
    spark.read.format('parquet').load(source_file_path_keys)
    .transform(fix_column_names)
    .select(*key_columns)
    .transform(trim_all_values)
    .distinct()  
  )
  
  apply_soft_delete(full_keys_f, table_name, key_columns_list)

# COMMAND ----------

# RETURN MAX VALUE
max_value = get_incr_col_max_value(main, incremental_column, incremental_type)
dbutils.notebook.exit(json.dumps({"max_value": max_value}))
