# Databricks notebook source
# DEBUG

# dbutils.widgets.removeAll()
# dbutils.widgets.text('config', '', '')

# {"database_name":"amazusftp1","overwrite":"false","sampling":"false","source_folder":"/datalake/AMAZUSFTP1/raw_data/delta_data","target_folder":"/datalake/AMAZUSFTP1/raw_data/full_data","test_run":"false","partitionkey":"1","rowkey":"4","timestamp":"2023-02-10T11:42:41.365835+00:00","active":"true","file_name":"WC_QV_POS_EXCLUDED_CUSTOMER_H.CSV","folder_name":"Skybot/","handle_delete":"false","header":"true","key_columns":"","publish":"true","table_name":"WC_QV_POS_EXCLUDED_CUSTOMER_H"}

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.3/header_b_amazusftp1

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

# COMMAND ----------

# BASIC CLEANUP
main_f = (
  main
  .transform(fix_column_names)
  .transform(convert_nan_to_null())
  .transform(trim_all_values)
  .transform_if((partition_column is not None), attach_partition_column(partition_column))
  .transform(attach_modified_date())
  .transform(attach_deleted_flag())
)

main_f.count()

# COMMAND ----------

# LOAD
options = {'container_name': target_container, 'storage_name': target_storage}

if (key_columns is None):
  options['overwrite'] = True
  register_hive_table(main_f, table_name, target_folder, options = options)
  append_into_table(main_f, table_name)
else:
  options['overwrite'] = overwrite
  register_hive_table(main_f, table_name, target_folder, options = options)
  merge_into_table(main_f, table_name, key_columns, options = {'auto_merge': True})

# COMMAND ----------

# HANDLE DELETE
if handle_delete and (key_columns is not None):
  
  key_columns_list = key_columns.split(',')  
  full_keys_f = (
    main
    .transform(fix_column_names)
    .transform(convert_nan_to_null())
    .transform(trim_all_values)
    .select(*key_columns_list)    
    .cache()
  )
  
  apply_soft_delete(full_keys_f, table_name, key_columns)

# COMMAND ----------

# CHECK ROW COUNT
check_row_count(table_name, main_f)

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.3/footer_b_amazusftp1

# COMMAND ----------

# RETURN MAX VALUE
max_value = get_max_value(main_f)
dbutils.notebook.exit(json.dumps({"max_value": max_value}))
