# Databricks notebook source
# MAGIC %run ../../FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../../PARTIALS/1.2/header_bronze_notebook_csv

# COMMAND ----------

# INPUT PARAMETERS
print_bronze_csv_params()

# COMMAND ----------

# VALIDATE SOURCE DETAILS
if source_folder is None or file_name is None:
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

# SETUP VARIABLES
table_name = get_table_name(database_name, None, table_name)
source_file_path = get_file_path(source_folder, file_name, target_container, target_storage)

print('table_name: ' + table_name)
print('file_name: ' + file_name)
print('source_file_path: ' + source_file_path)

if handle_delete:  
  if not incremental:
    source_folder_keys = source_folder
  source_file_path_keys = get_file_path(source_folder_keys, file_name, target_container, target_storage)
  print('source_file_path_keys: ' + source_file_path_keys)

# COMMAND ----------

# READ DATA
main = (
  spark.read
  .format('csv')
  .option("header", header)
  .option("escape", escape_char)
  .option("quote", "\"")
  .option("multiline", "true")
  .load(source_file_path)
)

main.display()

# COMMAND ----------

# SAMPLING
if sampling:
  main = main.limit(10)

# COMMAND ----------

# BASIC CLEANUP
main2 = (
  main
  .transform(fix_column_names)
  .transform(convert_nan_to_null)
  .transform(trim_all_values)
  .distinct()
)
display(main2)

# COMMAND ----------

# PARTITION DATA
if partition_column is not None:
  main3 = main2.transform(attach_partition_column(partition_column))
  partition_field = "_PART"
else:
  main3 = main2
  partition_field = None

# COMMAND ----------

main_f = (
  main3
  .withColumn('_DELETED', f.lit(False))
  .withColumn('_MODIFIED', f.current_timestamp())
)

# COMMAND ----------

# FINAL DATASET
main_f.cache()
main_f.display()

# COMMAND ----------

# VALIDATE DATA
if key_columns is not None:
  valid_count_rows(main_f, key_columns)

# COMMAND ----------

options = {
  'append_only': append_only,
  'incremental': incremental,
  'incremental_column': incremental_column,
  'overwrite': overwrite,
  'partition_column': partition_field,
  'sampling': sampling,
  'target_container': target_container,
  'target_storage': target_storage
}

merge_raw_to_delta(main_f, table_name, key_columns, target_folder, options)

# COMMAND ----------

# HANDLE DELETE
if handle_delete and (key_columns is not None) and not sampling:
  
  full_keys = (
    spark.read
    .format('csv')
    .option("header", header)
    .option("escape", "\\")
    .option("quote", "\"")
    .option("multiline", "true")
    .load(source_file_path_keys)    
  )
  
  key_columns_list = key_columns.split(',') 
  full_keys_f = (
    full_keys
    .transform(fix_column_names)
    .select(*key_columns_list)
    .transform(convert_nan_to_null)
    .transform(trim_all_values)
    .distinct()
    .cache()
  )
  
  if not hard_delete:
    apply_soft_delete(full_keys_f, table_name, key_columns)
    
  if hard_delete:
    apply_hard_delete(full_keys_f, table_name, key_columns)

# COMMAND ----------

# CHECK ROW COUNT
if handle_delete and (key_columns is not None) and not sampling:
  check_row_count(table_name, full_keys_f)

elif not incremental and not sampling:
  check_row_count(table_name, main_f)

# COMMAND ----------

# RETURN MAX VALUE
max_value = get_incr_col_max_value(main, incremental_column)
dbutils.notebook.exit(json.dumps({"max_value": max_value}))
