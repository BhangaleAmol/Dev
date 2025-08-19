# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap 

# COMMAND ----------

# INPUT PARAMETERS
database_name = get_input_param('database_name', default_value = 's_callsystems')
key_columns = get_input_param('key_columns', 'list', default_value = ['COMPANY_ID'])
overwrite = get_input_param('overwrite', 'bool', default_value = False)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'dim_company')
target_folder = get_input_param('target_folder', default_value = '/datalake_silver/callsystems/full_data')
temp_folder = get_input_param('temp_folder', default_value = '/datalake_silver/callsystems/temp_data')

# COMMAND ----------

# EXTRACT
file_path = get_file_path(temp_folder, 'clean_sf_account.dlt')
sf_account_df = spark.read.format('delta').load(file_path)
sf_account_df.createOrReplaceTempView("sf_account_df")

# COMMAND ----------

# SAMPLING
if sampling:
  sf_account_df = sf_account_df.limit(10)
  sf_account_df.createOrReplaceTempView('sf_account_df')

# COMMAND ----------

main_df = (
  sf_account_df
  .withColumnRenamed('ACCOUNT_ID', 'COMPANY_ID_NK')
  .withColumnRenamed('ACCOUNT_NAME', 'COMPANY_NAME')
  .withColumnRenamed('BILLING_COUNTRY', 'COUNTRY')
  .drop('LAST_MODIFIED_DATE')
)

main_df.display()

# COMMAND ----------

# ETL FIELDS
main_f = (
  main_df
  .transform(attach_source_column('SF'))
  .transform(attach_deleted_flag())
  .transform(attach_modified_date())
  .transform(attach_surrogate_key(['COMPANY_ID_NK', '_SOURCE'], name = 'COMPANY_ID'))
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
