# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap 

# COMMAND ----------

# MAGIC %run ./_SHARED/func_cti

# COMMAND ----------

# MAGIC %run ./_SHARED/func_salesforce

# COMMAND ----------

# INPUT PARAMETERS
temp_folder = get_input_param('temp_folder', default_value = '/datalake_silver/callsystems/temp_data')
table_name = get_input_param('table_name', default_value = 'clean_sf_contact')

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

sf_contact_df = spark.table('sf.contact')
sf_contact_df.createOrReplaceTempView("sf_contact_df")

# COMMAND ----------

sf_contact_df2 = spark.sql("""
  SELECT DISTINCT 
    c.id AS CONTACT_ID,
    c.accountid AS ACCOUNT_ID,
    c.phone AS PHONE, 
    c.mobilephone AS MOBILE_PHONE,
    c.lastmodifieddate AS LAST_MODIFIED_DATE,
    a.name AS COMPANY_NAME,
    a.billingcountry AS BILLING_COUNTRY
  FROM sf_contact_df c
  LEFT JOIN sf_account_df a ON c.accountid = a.id
  WHERE c.IsDeleted = false
""")

# COMMAND ----------

main_f = (
  sf_contact_df2
  .transform(add_sf_phone_number)
  .transform(add_sf_mobile_number)
  .transform(clean_sf_all_numbers(["PHONE_NUMBER", "MOBILE_NUMBER"]))
  .transform(clean_sf_phone_number)
  .transform(clean_sf_mobile_number)
  .drop("PHONE", "MOBILE_PHONE", "LAST_MODIFIED_DATE", "COMPANY_NAME", "BILLING_COUNTRY")  
)
main_f.display()

# COMMAND ----------

# LOAD
main_f.write.format('delta').mode('overwrite').save(target_file)

# COMMAND ----------

dbutils.notebook.exit("Success")
