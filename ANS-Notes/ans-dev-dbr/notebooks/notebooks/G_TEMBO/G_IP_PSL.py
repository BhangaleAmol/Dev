# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap 

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_tembo')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'string', default_value = 'Parent_Item_ID,Parent_Location, Structure_Sequence_Number')
overwrite = get_input_param('overwrite', 'bool', default_value = True)
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'IP_PSL')
target_folder = get_input_param('target_folder', default_value = '/datalake_gold/tembo/full_data')
temp_folder = get_input_param('temp_folder', default_value = '/datalake_gold/tembo/temp_data')

# COMMAND ----------

# EXTRACT
file_path = get_file_path(temp_folder, 'tmp_ip_psl.dlt')
TMP_IP_PSL = spark.read.format('delta').load(file_path)
TMP_IP_PSL.createOrReplaceTempView("TMP_IP_PSL")


# COMMAND ----------

# SAMPLING
if sampling:
  TMP_IP_PSL = TMP_IP_PSL.limit(10)
  TMP_IP_PSL.createOrReplaceTempView('TMP_IP_PSL')

# COMMAND ----------

# TRANSFORM
main_df = spark.sql("""
  SELECT 
     Parent_Item_ID,
     Parent_Location,
     Structure_Sequence_Number,
     Component_Item_ID,
     Component_Location,
     Usage_Rate,
     Popularity,
     Begin_Effective_Date,
     End_Effective_Date
  FROM
    TMP_IP_PSL
 """) 
main_df.display()

# COMMAND ----------

# LOAD
full_name = get_table_name(database_name, table_name)
register_hive_table(main_df, full_name, target_folder, options = {'overwrite': overwrite})
merge_into_table(main_df, full_name, key_columns)
