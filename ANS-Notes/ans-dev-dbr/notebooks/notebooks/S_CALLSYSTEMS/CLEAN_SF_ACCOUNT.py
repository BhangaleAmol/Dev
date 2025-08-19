# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap 

# COMMAND ----------

# MAGIC %run ./_SHARED/func_cti

# COMMAND ----------

# MAGIC %run ./_SHARED/func_salesforce

# COMMAND ----------

# INPUT PARAMETERS
temp_folder = get_input_param('temp_folder', default_value = '/datalake_silver/callsystems/temp_data')
table_name = get_input_param('table_name', default_value = 'clean_sf_account')

# COMMAND ----------

# SETUP PARAMETERS
file_name = get_file_name(table_name, 'dlt')
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# COMMAND ----------

# EXTRACT
sf_account_df = spark.table('sf.account')
sf_account_df.createOrReplaceTempView("sf_account_df")

# COMMAND ----------

sf_account_df2 = spark.sql("""
  SELECT DISTINCT 
    id AS ACCOUNT_ID, 
    name AS ACCOUNT_NAME,
    billingcountry AS BILLING_COUNTRY,
    lastmodifieddate AS LAST_MODIFIED_DATE
  FROM sf_account_df
  WHERE IsDeleted = false
""")

# COMMAND ----------

# TRANSFORM
main_f = (
  sf_account_df2
  .distinct()
  .transform(clean_sf_account_name)
  .filter(f.col("ACCOUNT_NAME").isNotNull())
  .transform(clean_sf_billing_country) 
  .transform(replace_nulls_with_unknown(['BILLING_COUNTRY']))
)
main_f.display()

# COMMAND ----------

# LOAD
main_f.write.format('delta').mode('overwrite').save(target_file)

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


