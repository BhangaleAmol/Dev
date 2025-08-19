# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap 

# COMMAND ----------

# MAGIC %run ./_SHARED/func_cti

# COMMAND ----------

# INPUT PARAMETERS
temp_folder = get_input_param('temp_folder', default_value = '/datalake_silver/callsystems/temp_data')
table_name = get_input_param('table_name', default_value = 'clean_cars')

# COMMAND ----------

# SETUP PARAMETERS
file_name = get_file_name(table_name, 'dlt')
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# COMMAND ----------

# EXTRACT
cars_df = spark.table('cti.cars')

# COMMAND ----------

# TRANSFORM
main_f = (
  cars_df
  .transform(rename_column_to_title("_"))
  .transform(rename_column("Accountid", "ACCOUNT_ID"))
  .transform(rename_column("Customername", "CUSTOMER_NAME"))
  .transform(rename_column("Customeremailaddress", "CUSTOMER_EMAIL_ADDRESS"))
  .transform(replace_null_string_with_null)
  .transform(replace_na_string_with_null)
  .transform(epoch_to_timestamp([
    "REALTIME_UPDATE_TIMESTAMP", 
    "HISTORICAL_UPDATE_TIMESTAMP", 
    "DATA_VALIDATOR_TIMESTAMP", 
    "CSTTS", 
    "CETTS"
  ]))
  .transform(replace_special_chars(['AGENT_NAME'], '?', ''))
  .transform(replace_special_chars(['AGENT_NAME'], '�', ''))
  .transform(cast_to_int([
    'TALK_DURATION',
    'QUEUE_DURATION',
    'HOLD_DURATION',
    'DURATION',
    'CTQ_DURATION',
    'CONFERENCE_DURATION',
    'WRAPUP_DURATION'   
  ]))
  .transform(replace_negative_with_zero([
    'TALK_DURATION',
    'QUEUE_DURATION',
    'HOLD_DURATION',
    'DURATION',
    'CTQ_DURATION',
    'CONFERENCE_DURATION',
    'WRAPUP_DURATION'
  ]))
)
main_f.display()

# COMMAND ----------

# LOAD
main_f.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

# COMMAND ----------

dbutils.notebook.exit("Success")
