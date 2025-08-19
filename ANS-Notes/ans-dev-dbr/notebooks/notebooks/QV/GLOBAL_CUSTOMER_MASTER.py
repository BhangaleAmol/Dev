# Databricks notebook source
# MAGIC %pip install --force-reinstall -v "openpyxl==3.1.0"

# COMMAND ----------

# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_bronze_notebook_xlsx

# COMMAND ----------

# DEFAULT PARAMETERS
source_folder = source_folder or '/datalake/QV/raw_data/delta_data'
target_folder = target_folder or '/datalake/QV/raw_data/full_data'
database_name = database_name or "qv"
file_name = file_name or "Global Customer Master.xlsx"
sheet_name = sheet_name or 'Global Customer'
table_name = table_name or "global_customer_master"
header = header or True
handle_delete = False

# COMMAND ----------

# INPUT PARAMETERS
print_bronze_xlsx_params()

# COMMAND ----------

# VALIDATE SOURCE DETAILS
if source_folder is None or file_name is None:
  raise Exception("Source data details are missing")

if handle_delete and source_folder_keys is None:
  raise Exception("Full keys folder is missing")
  
# VALIDATE TARGET DETAILS
if target_folder is None or database_name is None or table_name is None:
  raise Exception("Target details are missing")
  
# VALIDATE INCREMENTAL AND OVERWRITE
if incremental and overwrite:
  raise Exception("INCREMENTAL with OVERWRITE not allowed")

# COMMAND ----------

# SETUP VARIABLES
table_name = get_table_name(database_name, None, table_name)
source_file_path = '/dbfs/mnt/datalake{0}/{1}'.format(source_folder, file_name)

print('table_name: ' + table_name)
print('file_name: ' + file_name)
print('source_file_path: ' + source_file_path)

# COMMAND ----------

# READ DATA
header = None if header is False else 0
main_pd = pd.read_excel(source_file_path, header = header, sheet_name = sheet_name, usecols="A:AH", engine='openpyxl')
main_pd = main_pd.astype(str)
main_pd = main_pd.drop(main_pd.filter(regex='Unnamed:').columns, axis=1)
main = spark.createDataFrame(main_pd)
main.display()

# COMMAND ----------

# FIX DATA
main2 = (
  main
  .transform(fix_column_names)
  .transform(convert_nan_to_null)
  .transform(trim_all_values)
  .distinct()
)

main2.cache()
main2.display()

# COMMAND ----------

# DELETE FLAG
main_f = main2.withColumn('_DELETED', f.lit(False))

# COMMAND ----------

# SHOW DATA
main_f.cache()
main_f.display()

# COMMAND ----------

show_duplicate_rows(main_f, ['CustomerNumbers'])

# COMMAND ----------

key_columns = 'CustomerNumbers'

# COMMAND ----------

# DROP DUPLICATES
if key_columns is not None:
  main_f = main_f.dropDuplicates(['CustomerNumbers'])
else:
  main_f = main_f
  
main_f.display()

# COMMAND ----------

valid_count_rows(main_f, key_columns)

# COMMAND ----------

show_null_values(main_f)

# COMMAND ----------

options = {
  'append_only': append_only,
  'incremental': incremental,
  'incremental_column': incremental_column,
  'overwrite': overwrite,
  'sampling': sampling,
  'target_container': target_container,
  'target_storage': target_storage
}

merge_raw_to_delta(main_f, table_name, key_columns, target_folder, options)

# COMMAND ----------

# RETURN MAX VALUE
max_value = get_incr_col_max_value(main)
dbutils.notebook.exit(json.dumps({"max_value": max_value}))
