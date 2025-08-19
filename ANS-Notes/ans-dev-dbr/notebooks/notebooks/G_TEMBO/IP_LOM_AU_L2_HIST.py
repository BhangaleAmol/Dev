# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap 

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_tembo')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'string', default_value = 'Forecast_1_ID,Forecast_2_ID')
overwrite = get_input_param('overwrite', 'bool', default_value = True)
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'IP_LOM_AU_L2_HIST')
target_folder = get_input_param('target_folder', default_value = '/datalake_gold/tembo/full_data')
temp_folder = get_input_param('temp_folder', default_value = '/datalake_gold/tembo/temp_data')

# COMMAND ----------

# EXTRACT
file_path = get_file_path(temp_folder, 'tmp_ip_lom_au_l2_hist.dlt')
TMP_IP_LOM_AU_L2 = spark.read.format('delta').load(file_path)
TMP_IP_LOM_AU_L2.createOrReplaceTempView("TMP_IP_LOM_AU_L2")


# COMMAND ----------

# SAMPLING
if sampling:
  TMP_IP_LOM_AU_L2 = TMP_IP_LOM_AU_L2.limit(10)
  TMP_IP_LOM_AU_L2.createOrReplaceTempView('TMP_IP_LOM_AU_L2')

# COMMAND ----------

# TRANSFORM
main_df = spark.sql("""
  SELECT 
    Pyramid_Level,
    Forecast_1_ID,
    Forecast_2_ID,
    Forecast_3_ID,
    Record_Type,
    User_Data_43
  FROM
    TMP_IP_LOM_AU_L2
  where
    Forecast_1_ID not like '%#Error%'
    and Forecast_2_ID not like '%#Error%'
 """) 
main_df.display()

# COMMAND ----------

# LOAD
full_name = get_table_name(database_name, table_name)
register_hive_table(main_df, full_name, target_folder, options = {'overwrite': overwrite})
merge_into_table(main_df, full_name, key_columns)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from g_tembo.ip_lom_au_l2 
