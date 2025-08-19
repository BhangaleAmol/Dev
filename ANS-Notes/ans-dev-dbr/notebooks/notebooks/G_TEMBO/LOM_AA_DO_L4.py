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
table_name = get_input_param('table_name', default_value = 'LOM_AA_DO_L4')
target_folder = get_input_param('target_folder', default_value = '/datalake_gold/tembo/full_data')
temp_folder = get_input_param('temp_folder', default_value = '/datalake_gold/tembo/temp_data')

# COMMAND ----------

# EXTRACT
file_path = get_file_path(temp_folder, 'tmp_lom_aa_do_l4.dlt')
TMP_LOM_AA_DO_L4 = spark.read.format('delta').load(file_path)
TMP_LOM_AA_DO_L4.createOrReplaceTempView("TMP_LOM_AA_DO_L4")


# COMMAND ----------

# SAMPLING
if sampling:
  TMP_LOM_AA_DO_L4 = TMP_LOM_AA_DO_L1_HIST.limit(10)
  TMP_LOM_AA_DO_L4.createOrReplaceTempView('TMP_LOM_AA_DO_L4')

# COMMAND ----------

main_df = spark.sql("""
  SELECT distinct
    Pyramid_Level,
    Forecast_1_ID,
    Forecast_2_ID,
    Forecast_3_ID,
    Record_Type,
    Description,
    Forecast_Planner
  FROM
   TMP_LOM_AA_DO_L4
 where 
   Forecast_1_ID not like '%#Error%'
   and Forecast_2_ID not like '%#Error%'
   and Forecast_3_ID not like '%#Error%'
   and Description not like '%#Error%'
   and Forecast_Planner not like '%#Error%'
""") 
main_df.display()

# COMMAND ----------

# LOAD
full_name = get_table_name(database_name, table_name)
register_hive_table(main_df, full_name, target_folder, options = {'overwrite': overwrite})
merge_into_table(main_df, full_name, key_columns)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from g_tembo.lom_aa_do_l4
