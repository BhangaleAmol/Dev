# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap 

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_tembo')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'string', default_value = 'Item_Index')
overwrite = get_input_param('overwrite', 'bool', default_value = True)
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'LGTY_ITEM')
target_folder = get_input_param('target_folder', default_value = '/datalake_gold/tembo/full_data')
temp_folder = get_input_param('temp_folder', default_value = '/datalake_gold/tembo/temp_data')

# COMMAND ----------

# EXTRACT
file_path = get_file_path(temp_folder, 'tmp_lgty_item.dlt')
TMP_LGTY_ITEM = spark.read.format('delta').load(file_path)
TMP_LGTY_ITEM.createOrReplaceTempView("TMP_LGTY_ITEM")


# COMMAND ----------

# SAMPLING
if sampling:
  TMP_LGTY_ITEM = TMP_LGTY_ITEM.limit(10)
  TMP_LGTY_ITEM.createOrReplaceTempView('TMP_LGTY_ITEM')

# COMMAND ----------

# TRANSFORM
main_df = spark.sql("""
  SELECT distinct
    Item_Index,
    Item_ID,
    Item_Description, 
    Item_Abbreviated_Text,
    Unit_of_Measure_Index
  FROM
   TMP_LGTY_ITEM
 where 
   _PDHPRODUCTFLAG is null
  
""") 
main_df.display()

# COMMAND ----------

# LOAD
full_name = get_table_name(database_name, table_name)
register_hive_table(main_df, full_name, target_folder, options = {'overwrite': overwrite})
merge_into_table(main_df, full_name, key_columns)
