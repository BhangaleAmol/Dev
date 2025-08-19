# Databricks notebook source
# DEBUG

# dbutils.widgets.removeAll()
# dbutils.widgets.text('config', '', '')

# {"database_name":"col","overwrite":"false","sampling":"false","source_folder":"/datalake/COL/raw_data/delta_data","target_folder":"/datalake/COL/raw_data/full_data","test_run":"false","partitionkey":"1","rowkey":"7","timestamp":"2023-02-10T11:42:46.343+00:00","active":"true","handle_delete":"false","incremental":"false","publish":"true","source_name":"VW_QV_Invoices","table_name":"VW_QV_INVOICES"}

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_b_col

# COMMAND ----------

# READ DATA
main_1 = spark.read.format('parquet').load(source_file_path)
main_1.count()

# COMMAND ----------

# BASIC CLEANUP
main_2 = (
  main_1
  .transform(fix_column_names)
  .transform(trim_all_values)
  .distinct()
)

main_2.count()

# COMMAND ----------

main_f = (
  main_2
  .withColumn('_DELETED', f.lit(False))
  .withColumn('_MODIFIED', f.current_timestamp())
)

main_f.cache()
main_f.count()

# COMMAND ----------

options = {  
  'overwrite': overwrite,  
  'target_container': target_container,
  'target_storage': target_storage
}

merge_raw_to_delta(main_f, table_name, key_columns, target_folder, options)

# COMMAND ----------

# HANDLE DELETE
if handle_delete and (key_columns is not None):
  full_keys_f = (
    spark.read.format('parquet').load(source_file_path_keys)
    .transform(fix_column_names)
    .select(*key_columns)
    .transform(trim_all_values)
    .distinct()
    .cache()
  )
  
  apply_soft_delete(full_keys_f, table_name, key_columns)

# COMMAND ----------

# CHECK ROW COUNT
check_row_count(table_name, main_f)

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_b_col

# COMMAND ----------

# RETURN MAX VALUE
max_value = get_incr_col_max_value(main_1)
dbutils.notebook.exit(json.dumps({"max_value": max_value}))
