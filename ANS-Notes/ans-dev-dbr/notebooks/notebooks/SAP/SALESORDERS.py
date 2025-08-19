# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_bronze_notebook_csv

# COMMAND ----------

# DEFAULT PARAMETERS
source_folder = source_folder or '/datalake/SAP/raw_data/delta_data'
target_folder = target_folder or '/datalake/SAP/raw_data/full_data'
database_name = database_name or "sap"
file_name = file_name or "SalesOrders.csv"
table_name = table_name or "SalesOrders"
handle_delete = False

# COMMAND ----------

# INPUT PARAMETERS
print_bronze_csv_params()

# COMMAND ----------

# VALIDATE SOURCE DETAILS
if source_folder is None or file_name is None:
  raise Exception("Source data details are missing")

if handle_delete and source_folder_keys is None:
  raise Exception("Full keys folder is missing")
  
# VALIDATE TARGET DETAILS
if target_folder is None or database_name is None or table_name is None:
  raise Exception("Target details are missing")

# VALIDATE INCREMENTAL
if incremental and key_columns is None:
  raise Exception("INCREMENTAL operation not possible")
  
# VALIDATE HANDLE DELETE
if handle_delete and (key_columns is None or append_only is True):
  raise Exception("HANDLE DELETE operation not possible")
  
# VALIDATE INCREMENTAL AND OVERWRITE
if incremental and overwrite:
  raise Exception("INCREMENTAL with OVERWRITE not allowed")

# COMMAND ----------

# SETUP VARIABLES
endpoint_name = get_endpoint_name(target_container, target_storage)
table_name = get_table_name(database_name, None, table_name)
source_file_path = '{0}{1}/{2}'.format(endpoint_name, source_folder, file_name)

print('table_name: ' + table_name)
print('file_name: ' + file_name)
print('source_file_path: ' + source_file_path)

# COMMAND ----------

# READ DATA
main = (
  spark.read
  .format('csv')
  .option("header", header)
  .option("escape", "\\")
  .option("quote", "\"")
  .option("multiline", "true")
  .load(source_file_path)
)
main.display()

# COMMAND ----------

# FIX DATA
main2 = (
  main
    .withColumnRenamed('$GBU', 'GBU2')
    .withColumnRenamed('$GBUshort', 'GBUshort2')
    .transform(fix_column_names)
    .transform(convert_nan_to_null)
    .transform(trim_all_values)
    .distinct()
)

main2.cache()
main2.display()

# COMMAND ----------

# PARTITION DATA
if partition_column is not None:
  main3 = main2.transform(attach_partition_column(partition_column))
  partition_field = "_PART"
else:
  main3 = main2
  partition_field = None

# COMMAND ----------

# DELETE FLAG
main_f = main3.withColumn('_DELETED', f.lit(False))

# COMMAND ----------

# SHOW DATA
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

# RETURN MAX VALUE
max_value = get_incr_col_max_value(main, incremental_column)
dbutils.notebook.exit(json.dumps({"max_value": max_value}))

# COMMAND ----------


