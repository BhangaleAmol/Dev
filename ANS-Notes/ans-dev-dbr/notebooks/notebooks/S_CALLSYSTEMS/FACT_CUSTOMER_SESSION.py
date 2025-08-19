# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap 

# COMMAND ----------

# INPUT PARAMETERS
database_name = get_input_param('database_name', default_value = 's_callsystems')
key_columns = get_input_param('key_columns', 'list', default_value = ['CUSTOMER_SESSION_ID'])
overwrite = get_input_param('overwrite', 'bool', default_value = False)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'fact_customer_session')
target_folder = get_input_param('target_folder', default_value = '/datalake_silver/callsystems/full_data')
temp_folder = get_input_param('temp_folder', default_value = '/datalake_silver/callsystems/temp_data')

# COMMAND ----------

# EXTRACT
file_path = get_file_path(temp_folder, 'clean_aars.dlt')
aars_df = spark.read.format('delta').load(file_path)
aars_df.createOrReplaceTempView("aars_df")

file_path = get_file_path(temp_folder, 'clean_csrs.dlt')
csrs_df = spark.read.format('delta').load(file_path)
csrs_df.createOrReplaceTempView("csrs_df")

file_path = get_file_path(temp_folder, 'company_bridge.dlt')
company_bridge_df = spark.read.format('delta').load(file_path)
company_bridge_df.createOrReplaceTempView("company_bridge_df")

# COMMAND ----------

# SAMPLING
if sampling: 
  csrs_df = csrs_df.limit(10)
  csrs_df.createOrReplaceTempView("csrs_df")

# COMMAND ----------

# AARS
aars_f = spark.sql("""
  SELECT * FROM (
    SELECT CALL_SESSION_ID, AGENT_SYSTEM_ID, UPPER(TYPE) AS TYPE, DURATION
    FROM aars_df
    WHERE 
      TYPE IS NOT NULL 
      AND TYPE != 'null'
      AND CALL_SESSION_ID IS NOT NULL 
      AND CALL_SESSION_ID != 'null'
  )
  PIVOT (
    SUM(DURATION)
    FOR TYPE IN (
      'CONFERENCE' AGENT_CONFERENCE_DURATION, 
      'CONSULT-ANSWER' AGENT_CONSULT_ANSWER_DURATION, 
      'CONSULT-REQUEST' AGENT_CONSULT_REQUEST_DURATION, 
      'NOTRESPONDING' AGENT_NOTRESPONDING_DURATION, 
      'ON-HOLD' AGENT_HOLD_DURATION, 
      'RINGING' AGENT_RINGING_DURATION, 
      'TALKING' AGENT_TALK_DURATION, 
      'WRAPUP' AGENT_WRAPUP_DURATION    
    )
  )
""")
aars_f.createOrReplaceTempView('aars_f')
aars_f.cache()
aars_f.display()

# COMMAND ----------

# COMPANY BRIDGE
company_bridge_f = (
  company_bridge_df
  .withColumnRenamed('COMPANY_ID', 'COMPANY_ID_NK')
  .drop('COMPANY_ID')
  .transform(attach_source_column("SF"))
  .transform(attach_surrogate_key('COMPANY_ID_NK,_SOURCE', name = 'COMPANY_ID')))

company_bridge_f.display()
company_bridge_f.createOrReplaceTempView("company_bridge_f")

# COMMAND ----------

main_df = spark.sql("""
  SELECT 
    c.SID AS CUSTOMER_SESSION_ID_NK,
    c.AGENT_SYSTEM_ID AS AGENT_SYSTEM_ID_NK,
    c.ENTRYPOINT_SYSTEM_ID AS ENTRYPOINT_SYSTEM_ID_NK,    
    c.QUEUE_SYSTEM_ID AS QUEUE_SYSTEM_ID_NK,
    c.SITE_SYSTEM_ID AS SITE_SYSTEM_ID_NK,
    c.TEAM_SYSTEM_ID AS TEAM_SYSTEM_ID_NK,
    cb.COMPANY_ID AS COMPANY_ID,
    CAST(REPLACE(TO_DATE(c.CSTTS), "-", "") AS INTEGER) AS DATE_ID,
    COALESCE(c.CALL_DIRECTION, 'Unknown') AS CALL_DIRECTION, 
    CAST(c.CETTS AS TIMESTAMP),
    CAST(c.CSTTS AS TIMESTAMP),
    c.ANI AS SRC_PHONE_NUMBER,
    c.DNIS AS DST_PHONE_NUMBER,
    c.HANDLED,
    COALESCE(c.TERMINATING_END, 'Unknown') AS TERMINATING_END, 
    COALESCE(c.TERMINATION_TYPE, 'Unknown') AS TERMINATION_TYPE, 
    c.CONFERENCE_COUNT, 
    c.CONFERENCE_DURATION,
    c.CTQ_COUNT, 
    c.CTQ_DURATION,
    c.DELETED_RECORD, 
    c.DURATION,
    c.HOLD_COUNT, 
    c.HOLD_DURATION,
    c.IVR_COUNT, 
    c.IVR_DURATION,
    c.QUEUE_COUNT, 
    c.QUEUE_DURATION,
    c.TALK_COUNT, 
    c.TALK_DURATION, 
    c.TERMINATION_COUNT,
    c.WRAPUP_DURATION,
    COALESCE(c.WRAPUP_CODE_NAME, "Unknown") AS WRAPUP_CODE_NAME,
    a.AGENT_CONFERENCE_DURATION, 
    a.AGENT_CONSULT_ANSWER_DURATION, 
    a.AGENT_CONSULT_REQUEST_DURATION, 
    a.AGENT_NOTRESPONDING_DURATION, 
    a.AGENT_HOLD_DURATION, 
    a.AGENT_RINGING_DURATION, 
    a.AGENT_TALK_DURATION, 
    a.AGENT_WRAPUP_DURATION,
    c.CAD_SURVEY_QUESTION_1,
    c.CAD_SURVEY_QUESTION_2
  FROM csrs_df c
  LEFT JOIN aars_f a ON c.SID = a.CALL_SESSION_ID AND c.AGENT_SYSTEM_ID = a.AGENT_SYSTEM_ID
  LEFT JOIN company_bridge_f cb ON c.SID = CB.SID
""") 

main_df.display()

# COMMAND ----------

# ETL FIELDS
main_f = (
  main_df
  .transform(attach_source_column('CTI'))
  .transform(attach_deleted_flag())
  .transform(attach_modified_date())
  .transform(attach_surrogate_key(['CUSTOMER_SESSION_ID_NK','_SOURCE'], name = 'CUSTOMER_SESSION_ID'))
  .transform(attach_surrogate_key(columns = 'AGENT_SYSTEM_ID_NK,_SOURCE', name = 'AGENT_ID'))
  .transform(attach_surrogate_key(columns = 'ENTRYPOINT_SYSTEM_ID_NK,_SOURCE', name = 'ENTRYPOINT_ID'))
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

# COMMAND ----------


