# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap 

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_tembo')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'string', default_value = 'ITEM_ID,LOCATION_ID')
overwrite = get_input_param('overwrite', 'bool', default_value = True)
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'LGTY_ITEM_LOCATION')
target_folder = get_input_param('target_folder', default_value = '/datalake_gold/tembo/full_data')
temp_folder = get_input_param('temp_folder', default_value = '/datalake_gold/tembo/temp_data')

# COMMAND ----------

# EXTRACT
file_path = get_file_path(temp_folder, 'tmp_lgty_item_location.dlt')
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
    Item_Id,
    Location_ID,
    SP_Transfer_Out_Minimum_Quantity,
    SP_Transfer_Out_Mulitiple,
    SP_Transfer_Out_Max_Quantity,
    Schedule_Type
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
# MAGIC -- Location_ID,
# MAGIC --_source,
# MAGIC -- count(*) 
# MAGIC *
# MAGIC from g_tembo.LGTY_ITEM_LOCATION
# MAGIC -- group by Location_ID
# MAGIC --where source_location_1 like '%#Error%'
# MAGIC where item_id = '100010'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from g_tembo.LGTY_ITEM_LOCATION

# COMMAND ----------


