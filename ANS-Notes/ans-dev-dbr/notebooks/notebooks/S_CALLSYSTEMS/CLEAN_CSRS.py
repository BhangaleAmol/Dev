# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap 

# COMMAND ----------

# MAGIC %run ./_SHARED/func_cti

# COMMAND ----------

# INPUT PARAMETERS
temp_folder = get_input_param('temp_folder', default_value = '/datalake_silver/callsystems/temp_data')
table_name = get_input_param('table_name', default_value = 'clean_csrs')

# COMMAND ----------

# SETUP PARAMETERS
file_name = get_file_name(table_name, 'dlt')
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# COMMAND ----------

# EXTRACT
csrs_df = spark.table('cti.csrs')

# COMMAND ----------

# TRANSFORM
main_f = (
  csrs_df
  .transform(rename_column_to_title("_"))
  .transform(rename_column("Accountid", "ACCOUNT_ID"))
  .transform(rename_column("Deletedrecordtimestamp", "DELETED_RECORD_TIMESTAMP"))
  .transform(rename_column("Customername", "CUSTOMER_NAME"))
  .transform(rename_column("Customeremailaddress", "CUSTOMER_EMAIL_ADDRESS"))
  .transform(rename_column("Cad_Accountname", "CAD_ACCOUNT_NAME"))
  .transform(rename_column("Agentlogin", "AGENT_LOGIN"))
  .transform(rename_column("CAD_SURVEY__QUESTION_1","CAD_SURVEY_QUESTION_1"))
  .transform(rename_column("CAD_SURVEY__QUESTION_2","CAD_SURVEY_QUESTION_2"))
  .transform(replace_null_string_with_null)
  .transform(replace_na_string_with_null)
  .transform(epoch_to_timestamp([
    "REALTIME_UPDATE_TIMESTAMP", "HISTORICAL_UPDATE_TIMESTAMP", 
    "DATA_VALIDATOR_TIMESTAMP", "CSTTS", "CETTS", 
    "DELETED_RECORD_TIMESTAMP"
  ]))
  .transform(replace_special_chars(['AGENT_NAME'], '?', ''))
  .transform(replace_special_chars(['AGENT_NAME'], '�', ''))  
  .transform(cast_to_int([
    'CONFERENCE_COUNT', 'CONFERENCE_DURATION', 'CONSULT_DURATION',
    'CTQ_COUNT', 'CTQ_DURATION', 'DELETED_RECORD', 
    'DURATION', 'HANDLED', 'HOLD_COUNT', 
    'HOLD_DURATION', 'IVR_COUNT', 'IVR_DURATION',
    'QUEUE_COUNT', 'QUEUE_DURATION', 'TALK_COUNT', 
    'TALK_DURATION', 'TERMINATION_COUNT',
    'WRAPUP_DURATION'
  ]))
  .transform(replace_negative_with_zero([
    'TALK_DURATION',
    'QUEUE_DURATION',
    'IVR_DURATION',
    'HOLD_DURATION',
    'DURATION',
    'CTQ_DURATION',
    'CONSULT_DURATION',
    'CONFERENCE_DURATION',
    'WRAPUP_DURATION'   
  ]))
  .transform(clean_phone_numbers(["DNIS", "ANI"]))
)
main_f.display()

# COMMAND ----------

# LOAD
main_f.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

# COMMAND ----------

dbutils.notebook.exit("Success")
