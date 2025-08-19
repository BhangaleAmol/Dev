# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap 

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_tembo')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'string', default_value = 'Forecast_1_ID,Forecast_2_ID,Forecast_3_ID')
overwrite = get_input_param('overwrite', 'bool', default_value = True)
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'LOM_AU_DO_L1')
target_folder = get_input_param('target_folder', default_value = '/datalake_gold/tembo/full_data')
temp_folder = get_input_param('temp_folder', default_value = '/datalake_gold/tembo/temp_data')

# COMMAND ----------

# EXTRACT
file_path = get_file_path(temp_folder, 'tmp_lom_au_do_l1.dlt')
TMP_LOM_AU_DO_L1 = spark.read.format('delta').load(file_path)
TMP_LOM_AU_DO_L1.createOrReplaceTempView("TMP_LOM_AU_DO_L1")


# COMMAND ----------

# SAMPLING
if sampling:
  TMP_LOM_AU_DO_L1 = TMP_LOM_AU_DO_L1_HIST.limit(10)
  TMP_LOM_AU_DO_L1.createOrReplaceTempView('TMP_LOM_AU_DO_L1')

# COMMAND ----------

# TRANSFORM
main_df = spark.sql("""
  SELECT 
    Pyramid_Level,
    Forecast_1_ID,
    Forecast_2_ID,
    Forecast_3_ID,
    Record_Type,
    User_Data_34,
    User_Data_35,
    User_Data_38,
    User_Data_39,
    User_Data_41,
    min(User_Data_42) User_Data_42,
    User_Data_43,
    User_Data_44,
    min(User_Data_45) User_Data_45,
    User_Data_46,
    User_Data_47,
    User_Data_48
  FROM
   TMP_LOM_AU_DO_L1
 where 
   forecast_1_id not like '#Error%'
   and forecast_2_id not like '#Error%'
   and forecast_3_id not like '#Error%'
   and User_Data_34 not like '#Error%'
   and User_Data_35 not like '#Error%'
   and User_Data_38 not like '#Error%'
   and User_Data_39 not like '#Error%'
   and User_Data_41 not like '#Error%'
   and User_Data_42 not like '#Error%'
   and User_Data_43 not like '#Error%'
   and User_Data_44 not like '#Error%'
   and User_Data_45 not like '#Error%'
   and User_Data_46 not like '#Error%'
   and User_Data_47 not like '#Error%'
 group by 
    Pyramid_Level,
    Forecast_1_ID,
    Forecast_2_ID,
    Forecast_3_ID,
    Record_Type,
    User_Data_34,
    User_Data_35,
    User_Data_38,
    User_Data_39,
    User_Data_41,
    User_Data_43,
    User_Data_44,
     User_Data_46,
    User_Data_47,
    User_Data_48
""") 
main_df.display()

# COMMAND ----------

# LOAD
full_name = get_table_name(database_name, table_name)
register_hive_table(main_df, full_name, target_folder, options = {'overwrite': overwrite})
merge_into_table(main_df, full_name, key_columns)
