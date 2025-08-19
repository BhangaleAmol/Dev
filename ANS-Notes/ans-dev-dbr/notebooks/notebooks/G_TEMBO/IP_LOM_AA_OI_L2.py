# Databricks notebook source
############ THIS NOTEBOOK IS REDUNDANT ############

# COMMAND ----------

# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap 

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_tembo')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'string', default_value = 'Forecast_1_ID,Forecast_2_ID')
overwrite = get_input_param('overwrite', 'bool', default_value = True)
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'IP_LOM_AA_OI_L2')
table_name = table_name.lower()
target_folder = get_input_param('target_folder', default_value = '/datalake_gold/tembo/full_data')
temp_folder = get_input_param('temp_folder', default_value = '/datalake_gold/tembo/temp_data')

# COMMAND ----------

# EXTRACT
file_path = get_file_path(temp_folder, 'tmp_ip_lom_aa_oi_l2.dlt')
TMP_IP_LOM_AA_OI_L2 = spark.read.format('delta').load(file_path)
TMP_IP_LOM_AA_OI_L2.createOrReplaceTempView("TMP_IP_LOM_AA_OI_L2")


# COMMAND ----------

# SAMPLING
if sampling:
  TMP_IP_LOM_AA_OI_L2 = TMP_IP_LOM_AA_OI_L2.limit(10)
  TMP_IP_LOM_AA_OI_L2.createOrReplaceTempView('TMP_IP_LOM_AA_OI_L2')

# COMMAND ----------

# TRANSFORM
main_df = spark.sql("""
  SELECT 
    Pyramid_Level,
    Forecast_1_ID,
    Forecast_2_ID,
    Forecast_3_ID,
    Record_Type,
    Description,
    Forecast_calculate_indicator,
    Unit_price,
    Unit_cost,
    Unit_cube,
    Unit_weight,
    Product_group_conversion_option,
    Product_group_conversion_factor,
    Forecast_Planner,
    User_Data_84,
    User_Data_01,
    User_Data_02,
    User_Data_03,
    User_Data_04,
    User_Data_05,
    User_Data_06,
    User_Data_07,
    User_Data_08,
    User_Data_14,
    User_Data_15,
    User_Data_16,
    Unit_of_measure
  FROM
    TMP_IP_LOM_AA_OI_L2
 """) 
main_df.display()

# COMMAND ----------

# LOAD
# full_name = get_table_name(database_name, table_name)
# register_hive_table(main_df, full_name, target_folder, options = {'overwrite': overwrite})
# merge_into_table(main_df, full_name, key_columns)
