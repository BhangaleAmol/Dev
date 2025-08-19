# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap 

# COMMAND ----------

# INPUT PARAMETERS
database_name = get_input_param('database_name', default_value = 's_callsystems')
key_columns = get_input_param('key_columns', 'list', default_value = ['AGENT_ID'])
overwrite = get_input_param('overwrite', 'bool', default_value = False)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'dim_agent')
target_folder = get_input_param('target_folder', default_value = '/datalake_silver/callsystems/full_data')
temp_folder = get_input_param('temp_folder', default_value = '/datalake_silver/callsystems/temp_data')

# COMMAND ----------

# EXTRACT
file_path = get_file_path(temp_folder, 'clean_asrs.dlt')
asrs_df = spark.read.format('delta').load(file_path)
asrs_df.createOrReplaceTempView("asrs_df")

# COMMAND ----------

# SAMPLING
if sampling:
  asrs_df = asrs_df.limit(10)
  asrs_df.createOrReplaceTempView('asrs_df')

# COMMAND ----------

# TRANSFORM
main_df = spark.sql("""
  SELECT 
    AGENT_ID_NK,
    AGENT_SYSTEM_ID_NK,
    AGENT_NAME
  FROM (
    SELECT DISTINCT 
      AGENT_ID AS AGENT_ID_NK,
      AGENT_SYSTEM_ID AS AGENT_SYSTEM_ID_NK,
      AGENT_NAME,
      ROW_NUMBER() OVER (PARTITION BY AGENT_SYSTEM_ID ORDER BY CSTTS DESC) AS NUM
    FROM asrs_df
  ) 
  WHERE NUM = 1
""") 

main_df.display()

# COMMAND ----------

# ETL FIELDS
main_f = (
  main_df
  .transform(attach_source_column('CTI'))
  .transform(attach_deleted_flag())
  .transform(attach_modified_date())
  .transform(attach_surrogate_key(['AGENT_SYSTEM_ID_NK','_SOURCE'], name = 'AGENT_ID'))
  .transform(attach_unknown_record)
)

main_f.display()

# COMMAND ----------

# VALIDATE
check_distinct_count(main_f, key_columns)

# COMMAND ----------

# LOAD
full_name = get_table_name(database_name, table_name)
register_hive_table(main_f, full_name, target_folder, options = {'overwrite': overwrite})
merge_into_table(main_f, full_name, key_columns)

# COMMAND ----------


