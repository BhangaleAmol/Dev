# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap 

# COMMAND ----------

# INPUT PARAMETERS
database_name = get_input_param('database_name', default_value = 's_callsystems')
key_columns = get_input_param('key_columns', 'list', default_value = ['ACTIVITY_ID'])
overwrite = get_input_param('overwrite', 'bool', default_value = False)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'fact_agent_activity')
target_folder = get_input_param('target_folder', default_value = '/datalake_silver/callsystems/full_data')
temp_folder = get_input_param('temp_folder', default_value = '/datalake_silver/callsystems/temp_data')

# COMMAND ----------

# EXTRACT
file_path = get_file_path(temp_folder, 'clean_aars.dlt')
aars_df = spark.read.format('delta').load(file_path)
aars_df.createOrReplaceTempView("aars_df")

# COMMAND ----------

# SAMPLING
if sampling:
  aars_df = aars_df.limit(10)
  aars_df.createOrReplaceTempView('aars_df')

# COMMAND ----------

main_df = spark.sql("""
  SELECT 
    SID AS SID_NK,
    AGENT_SYSTEM_ID AS AGENT_SYSTEM_ID_NK,
    QUEUE_SYSTEM_ID AS QUEUE_SYSTEM_ID_NK,
    SITE_SYSTEM_ID AS SITE_SYSTEM_ID_NK,
    TEAM_SYSTEM_ID AS TEAM_SYSTEM_ID_NK,
    CAST(REPLACE(TO_DATE(CSTTS), "-", "") AS INTEGER) AS DATE_ID,
    AGENT_SESSION_ID AS AGENT_SESSION_ID_NK,
    CALL_SESSION_ID AS CUSTOMER_SESSION_ID_NK,
    CAST(CETTS AS TIMESTAMP) AS CETTS, 
    CAST(CSTTS AS TIMESTAMP) AS CSTTS,
    UPPER(TYPE) AS TYPE,    
    DURATION,
    CASE 
      WHEN UPPER(TYPE) = "IDLE" THEN COALESCE(IDLE_CODE_NAME, "Unknown")
      ELSE IDLE_CODE_NAME
    END AS IDLE_CODE_NAME,
    CASE 
      WHEN UPPER(TYPE) = "WRAPUP" THEN COALESCE(WRAPUP_CODE_NAME, "Unknown")
      ELSE WRAPUP_CODE_NAME
    END AS WRAPUP_CODE_NAME
  FROM aars_df
  WHERE TYPE IS NOT NULL AND TYPE != 'null'
""") 

main_df.display()

# COMMAND ----------

# ETL FIELDS
main_f = (
  main_df
  .transform(attach_source_column('CTI'))
  .transform(attach_deleted_flag())
  .transform(attach_modified_date())
  .transform(attach_surrogate_key(['SID_NK','_SOURCE'], name = 'ACTIVITY_ID'))
  .transform(attach_surrogate_key(columns = 'AGENT_SESSION_ID_NK,_SOURCE', name = 'AGENT_SESSION_ID'))
  .transform(attach_surrogate_key(columns = 'CUSTOMER_SESSION_ID_NK,_SOURCE', name = 'CUSTOMER_SESSION_ID'))
  .transform(attach_surrogate_key(columns = 'AGENT_SYSTEM_ID_NK,_SOURCE', name = 'AGENT_ID'))
  .transform(attach_surrogate_key(columns = 'QUEUE_SYSTEM_ID_NK,_SOURCE', name = 'QUEUE_ID'))
  .transform(attach_surrogate_key(columns = 'SITE_SYSTEM_ID_NK,_SOURCE', name = 'SITE_ID'))
  .transform(attach_surrogate_key(columns = 'TEAM_SYSTEM_ID_NK,_SOURCE', name = 'TEAM_ID'))
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
