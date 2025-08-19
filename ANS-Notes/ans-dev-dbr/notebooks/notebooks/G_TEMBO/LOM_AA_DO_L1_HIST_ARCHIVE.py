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
table_name = get_input_param('table_name', default_value = 'LOM_AA_DO_L1_HIST_ARCHIVE')
target_folder = get_input_param('target_folder', default_value = '/datalake_gold/tembo/full_data')
temp_folder = get_input_param('temp_folder', default_value = '/datalake_gold/tembo/temp_data')

# COMMAND ----------

# EXTRACT
file_path = get_file_path(temp_folder, 'tmp_lom_aa_do_l1.dlt')
TMP_LOM_AA_DO_L1_HIST = spark.read.format('delta').load(file_path)
TMP_LOM_AA_DO_L1_HIST.createOrReplaceTempView("TMP_LOM_AA_DO_L1_HIST")


# COMMAND ----------

# SAMPLING
if sampling:
  TMP_LOM_AA_DO_L1_HIST = TMP_LOM_AA_DO_L1_HIST.limit(10)
  TMP_LOM_AA_DO_L1_HIST.createOrReplaceTempView('TMP_LOM_AA_DO_L1_HIST')

# COMMAND ----------

# TRANSFORM
main_df = spark.sql("""
  SELECT distinct
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
    Unit_Weight,
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
    User_Data_09,
    User_Data_10,
    User_Data_11,
    User_Data_12,
    User_Data_13,
    User_Data_14,
    User_Data_15,
    User_Data_16,
    User_Data_17,
    User_Data_18,
    User_Data_19,
    User_Data_20,
    User_Data_21,
    User_Data_22,
    User_Data_23,
    User_Data_24,
    User_Data_25,
    User_Data_26,
    User_Data_27,
    User_Data_28,
    User_Data_29,
    User_Data_30,
    User_Data_31,
    User_Data_32,
    User_Data_33,
    Cube_unit_of_measure,
    Weight_unit_of_measure,
    Unit_of_measure,
    Case_quantity
  FROM
   TMP_LOM_AA_DO_L1_HIST
 where 
   forecast_1_id not like '#Error%'
   and forecast_2_id not like '#Error%'
   and forecast_3_id not like '#Error%'
   and description not like '#Error%'
   and nvl(User_Data_84,'Include') not like '#Error%'
   and nvl(User_Data_01,'Include') not like '#Error%'
   and nvl(User_Data_02,'Include') not like '#Error%'
   and nvl(User_Data_03,'Include') not like '#Error%'
   and nvl(User_Data_04,'Include') not like '#Error%'
   and nvl(User_Data_05,'Include') not like '#Error%'
   and nvl(User_Data_06,'Include') not like '#Error%'
   and nvl(User_Data_07,'Include') not like '#Error%'
   and nvl(User_Data_08,'Include') not like '#Error%'
   and nvl(User_Data_09,'Include') not like '#Error%'
   and nvl(User_Data_10,'Include') not like '#Error%'
   and nvl(User_Data_11,'Include') not like '#Error%'
   and nvl(User_Data_12,'Include') not like '#Error%'
   and nvl(User_Data_13,'Include') not like '#Error%'
   and nvl(User_Data_14,'Include') not like '#Error%'
   and nvl(User_Data_15,'Include') not like '#Error%'
   and nvl(User_Data_16,'Include') not like '#Error%'
   and nvl(User_Data_23,'Include') not like '#Error%'
   and nvl(User_Data_30,'Include') not like '#Error%'
   and nvl(User_Data_31,'Include') not like '#Error%'
   and nvl(User_Data_32,'Include') not like '#Error%'
   and nvl(User_Data_33,'Include') not like '#Error%'
""") 
main_df.display()

# COMMAND ----------

# LOAD
full_name = get_table_name(database_name, table_name)
register_hive_table(main_df, full_name, target_folder, options = {'overwrite': False})
merge_into_table(main_df, full_name, key_columns)
