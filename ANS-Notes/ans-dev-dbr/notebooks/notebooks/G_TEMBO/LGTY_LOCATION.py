# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap 

# COMMAND ----------

param_dict = {
  "table_name": "LGTY_LOCATION",
  "keys": "LOC_ID",
  
}

# COMMAND ----------

# INPUT
table_name = param_dict["table_name"].lower()

database_name = get_input_param('database_name', default_value = 'g_tembo')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'string', default_value = param_dict["keys"])
overwrite = get_input_param('overwrite', 'bool', default_value = True)
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = table_name)
table_name = table_name.lower()
target_folder = get_input_param('target_folder', default_value = '/datalake_gold/tembo/full_data')
temp_folder = get_input_param('temp_folder', default_value = '/datalake_gold/tembo/temp_data')

# COMMAND ----------

# EXTRACT
file_path = get_file_path(temp_folder, f'tmp_{table_name}.dlt')
tmp_df = spark.read.format('delta').load(file_path)
tmp_df.createOrReplaceTempView(f"tmp_{table_name}")


# COMMAND ----------

# SAMPLING
if sampling:
  tmp_df = tmp_df.limit(10)
  tmp_df.createOrReplaceTempView(f"tmp_{table_name}")

# COMMAND ----------

# TRANSFORM
main_df = spark.sql(f"""  
select * from tmp_{table_name}
""") 
main_df.display()

# COMMAND ----------

# LOAD
full_name = get_table_name(database_name, table_name)
register_hive_table(main_df, full_name, target_folder, options = {'overwrite': overwrite})
merge_into_table(main_df, full_name, key_columns)
