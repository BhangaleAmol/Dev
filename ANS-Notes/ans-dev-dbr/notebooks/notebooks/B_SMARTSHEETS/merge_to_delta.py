# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_bronze_notebook_ss

# COMMAND ----------

# INPUT PARAMETERS
print_bronze_ss_params()

# COMMAND ----------

# source_folder = "/datalake/SMARTSHEETS/raw_data/delta_data"
# target_folder = "/datalake/SMARTSHEETS/raw_data/full_data"
# database_name = "smartsheets"
# sheet_id = "4335358763657092"
# table_name ="QVPOS_Alt_EMS_Org_Zip"

# COMMAND ----------

# VALIDATE SOURCE DETAILS
if source_folder is None or table_name is None:
  raise Exception("Source data details are missing")
  
# VALIDATE TARGET DETAILS
if target_folder is None or database_name is None or table_name is None:
  raise Exception("Target details are missing")
  
# VALIDATE OVERWRITE
if incremental and overwrite:
  raise Exception("INCREMENTAL with OVERWRITE not allowed")
  

# COMMAND ----------

# SETUP VARIABLES
file_name = get_file_name(None, table_name).lower()
table_name = get_table_name(database_name, None, table_name)
source_file_path = get_file_path(source_folder, f'{file_name}.json', target_container, target_storage)

print('table_name: ' + table_name)
print('file_name: ' + file_name)
print('source_file_path: ' + source_file_path)

# COMMAND ----------

# READ DATA
spark.conf.set("spark.databricks.io.cache.enabled", "false")
df = spark.read.option("multiLine", "true").json(source_file_path)
df.printSchema()

# COMMAND ----------

# EXTRACT COLUMNS
columns = (
  df
  .select(f.explode("columns"))
  .select('col.*')
  .select('id', 'title')
)
columns.show(truncate=False)

# COMMAND ----------

# EXTRACT ROWS
rows = (
  df
  .select(f.explode("rows"))
  .select(f.col('col.rowNumber').alias('rowNumber'), f.explode("col.cells"))
  .select('rowNumber', 'col.*')
)
rows.show(truncate=False)

# COMMAND ----------

# JOIN DATASETS
main = (
  rows
  .join(columns, rows.columnId == columns.id)
  .select('rowNumber', 'value', 'title')
)
main.show()

# COMMAND ----------

# PIVOT ON COLUMN NAME
main2 = (
    main
    .groupby(main.rowNumber)
    .pivot("title")
    .agg(f.first("value"))    
)

main2.display()
main2.createOrReplaceTempView('main2')

# COMMAND ----------

if table_column_exists('main2','ACTIVE_FLG'):
  main3 = (
  main2
  .transform(convert_null_to_false('ACTIVE_FLG'))
  .withColumn('_DELETED', f.lit(False))
  .withColumn('_MODIFIED', f.current_timestamp())
    )
else:
  main3 = (
  main2
  .withColumn('_DELETED', f.lit(False))
  .withColumn('_MODIFIED', f.current_timestamp())
    )
  

# COMMAND ----------

# BASIC CLEANUP
main_f = (
  main3
  .transform(fix_column_names)
  .transform(trim_all_values)
#   .distinct()
)

main_f.display()

# COMMAND ----------

# PARTITION DATA
if partition_column is not None:
  main3 = main2.transform(attach_partition_column(partition_column))
  partition_field = '_PART'
else:
  main3 = main2
  partition_field = None

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

# RETURN MAX VALUE
max_value = get_incr_col_max_value(main2, None)
dbutils.notebook.exit(json.dumps({"max_value": max_value}))
