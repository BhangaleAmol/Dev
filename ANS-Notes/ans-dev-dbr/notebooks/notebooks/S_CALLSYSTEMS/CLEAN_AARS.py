# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ./_SHARED/func_cti

# COMMAND ----------

# INPUT PARAMETERS
temp_folder = get_input_param('temp_folder', default_value = '/datalake_silver/callsystems/temp_data')
table_name = get_input_param('table_name', default_value = 'clean_aars')

# COMMAND ----------

# SETUP PARAMETERS
file_name = get_file_name(table_name, 'dlt')
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# COMMAND ----------

# EXTRACT
aars_df = spark.table('cti.aars')

# COMMAND ----------

# TRANSFORM
main_f = (
  aars_df
  .transform(rename_column_to_title("_"))
  .transform(replace_null_string_with_null)
  .transform(replace_na_string_with_null)
  .transform(epoch_to_timestamp([
    "REALTIME_UPDATE_TIMESTAMP", 
    "HISTORICAL_UPDATE_TIMESTAMP", 
    "DATA_VALIDATOR_TIMESTAMP_AAR", 
    "CSTTS", 
    "CETTS"
  ])) 
  .transform(replace_special_chars(['AGENT_NAME'], '?', ''))
  .transform(replace_special_chars(['AGENT_NAME'], '�', ''))
  .transform(cast_to_int([
    'AVAILABLE_DURATION', 
    'DURATION', 
    'CONFERENCE_DURATION', 
    'CONSULT_DURATION', 
    'CTQ_DURATION', 
    'HOLD_DURATION', 
    'IDLE_DURATION', 
    'NOT_RESPONDING_DURATION', 
    'RINGING_DURATION', 
    'TALK_DURATION',
    'WRAPUP_DURATION'
  ]))
  .transform(replace_negative_with_zero([
    'AVAILABLE_DURATION', 
    'DURATION', 
    'CONFERENCE_DURATION', 
    'CONSULT_DURATION', 
    'CTQ_DURATION', 
    'HOLD_DURATION', 
    'IDLE_DURATION', 
    'NOT_RESPONDING_DURATION', 
    'RINGING_DURATION', 
    'TALK_DURATION',
    'WRAPUP_DURATION'
  ]))
)
main_f.display()

# COMMAND ----------

# LOAD
main_f.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

# COMMAND ----------

dbutils.notebook.exit("Success")
