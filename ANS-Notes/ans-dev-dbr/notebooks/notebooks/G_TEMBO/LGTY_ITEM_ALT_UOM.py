# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap 

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_tembo')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'string', default_value = 'ITEM_INDX,UOM_INDX')
overwrite = get_input_param('overwrite', 'bool', default_value = True)
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'LGTY_ITEM_ALT_UOM')
target_folder = get_input_param('target_folder', default_value = '/datalake_gold/tembo/full_data')
temp_folder = get_input_param('temp_folder', default_value = '/datalake_gold/tembo/temp_data')

# COMMAND ----------

# EXTRACT
file_path = get_file_path(temp_folder, 'tmp_lgty_item_alt_uom.dlt')
main = spark.read.format('delta').load(file_path)
main.createOrReplaceTempView("main")


# COMMAND ----------

# SAMPLING
if sampling:
  main = main.limit(10)
  main.createOrReplaceTempView('main')

# COMMAND ----------

# TRANSFORM
main_df = spark.sql("""
  SELECT 
    ITEM_INDX,
    UOM_INDX,
    UOM_CONV_FCTR
 FROM
    main
""") 
main_df.display()

# COMMAND ----------

# LOAD
full_name = get_table_name(database_name, table_name)
register_hive_table(main_df, full_name, target_folder, options = {'overwrite': overwrite})
merge_into_table(main_df, full_name, key_columns)

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC
# MAGIC --_source,
# MAGIC count(*) 
# MAGIC from g_tembo.LGTY_ITEM_ALT_UOM
# MAGIC --group by _source
# MAGIC --where source_location_1 like '%#Error%'

# COMMAND ----------


