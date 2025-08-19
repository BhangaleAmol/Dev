# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.customer_location

# COMMAND ----------

# LOAD DATASETS
main_inc = load_full_dataset('tot.tb_pbi_customer')

# COMMAND ----------

# SAMPLING
if sampling:
  main_inc = main_inc.limit(10)

# COMMAND ----------

# VIEWS
main_inc.createOrReplaceTempView('tb_pbi_customer')

# COMMAND ----------

tb_pbi_orders_f = spark.sql("""
  SELECT
    Customer_id,
    Company,
    MAX(Order_included_date) AS Order_included_date
  FROM tot.tb_pbi_orders
  GROUP BY Customer_id, Company
""")

tb_pbi_orders_f.createOrReplaceTempView('tb_pbi_orders_f')
tb_pbi_orders_f.cache()
tb_pbi_orders_f.count()

# COMMAND ----------

TMP_STATENAME_LKP = spark.sql("""select KEY_VALUE, CVALUE FROM smartsheets.edm_control_table where TABLE_ID = 'STATE_CODES'""")
TMP_STATENAME_LKP.createOrReplaceTempView('STATENAME_LKP')

# COMMAND ----------

main_billTo = spark.sql("""
  SELECT distinct
    CAST(NULL AS STRING) AS createdBy,
    CAST(o.Order_included_date AS TIMESTAMP) AS createdOn,
    CAST(NULL AS STRING) AS modifiedBy,
    CAST(NULL AS TIMESTAMP) AS modifiedOn,
    CURRENT_TIMESTAMP() AS insertedOn,
    CURRENT_TIMESTAMP() AS updatedOn, 
    CONCAT(c.Company, '-', c.Customer_id) AS accountId,
    CONCAT(c.Company, '-', c.Customer_id) AS accountNumber,
    CONCAT('BILL_TO-', c.Company, '-' , c.Customer_id) AS addressId, 
    c.Customer_address AS addressLine1,
    CAST(NULL AS STRING) AS addressLine2,
    CAST(NULL AS STRING) AS addressLine3,
    CAST(NULL AS STRING) AS addressLine4,
    c.Customer_city AS city,
    'BR' AS country,
    'BR' AS countryCode,
    NULL AS county,
    CAST(NULL AS STRING) AS emailAddress,
    CAST(NULL AS STRING) AS indiaIrpCustomerType,
    NULL AS mdmId,
    c.Customer_name AS name,
    CAST(NULL AS STRING) AS partySiteNumber,
    CAST(NULL AS STRING) AS phoneNumber,
    c.Customer_postal_cod AS postalCode,
    NULL AS primaryFlag,
    'LA' siteCategory,
    CAST(NULL AS STRING) AS siteUseId,
    'BILL_TO-' AS siteUseCode,  
    STATENAME_LKP.CVALUE AS stateName,
    c.Customer_state AS state
  FROM tb_pbi_customer c
  LEFT JOIN tb_pbi_orders_f o ON c.Customer_id = o.Customer_id and c.Company = o.Company
   LEFT JOIN STATENAME_LKP
        on STATENAME_LKP.KEY_VALUE = CONCAT('BR','-',trim(c.Customer_state) )
""")

display(main_billTo)

# COMMAND ----------

main_shipTo = spark.sql("""
  SELECT
    CAST(NULL AS STRING) AS createdBy,
    CAST(o.Order_included_date AS TIMESTAMP) AS createdOn,
    CAST(NULL AS STRING) AS modifiedBy,
    CAST(NULL AS TIMESTAMP) AS modifiedOn,
    CURRENT_TIMESTAMP() AS insertedOn,
    CURRENT_TIMESTAMP() AS updatedOn, 
    CONCAT(c.Company, '-', c.Customer_id) AS accountId,
    CONCAT(c.Company, '-', c.Customer_id) AS accountNumber,
    CONCAT('SHIP_TO-', c.Company, '-' , c.Customer_delivery) AS addressId,   
    c.Customer_address AS addressLine1,
    CAST(NULL AS STRING) AS addressLine2,
    CAST(NULL AS STRING) AS addressLine3,
    CAST(NULL AS STRING) AS addressLine4,
    c.Customer_city AS city,
    'BR' AS country,
    'BR' AS countryCode,
    NULL AS county,
    CAST(NULL AS STRING) AS emailAddress,
    CAST(NULL AS STRING) AS indiaIrpCustomerType,
    NULL AS mdmId,
    c.Customer_name AS name,
    CAST(NULL AS STRING) AS partySiteNumber,
    CAST(NULL AS STRING) AS phoneNumber,
    c.Customer_postal_cod AS postalCode,
    NULL AS primaryFlag,
    'LA' siteCategory,
    CAST(NULL AS STRING) AS siteUseId,
    'SHIP_TO-' AS siteUseCode, 
    STATENAME_LKP.CVALUE AS stateName,
    c.Customer_state AS state
  FROM tb_pbi_customer c
  LEFT JOIN tb_pbi_orders_f o ON c.Customer_id = o.Customer_id and c.Company = o.Company
  LEFT JOIN STATENAME_LKP
       on STATENAME_LKP.KEY_VALUE = CONCAT('BR','-',trim(c.Customer_state) )
""")

display(main_shipTo)

# COMMAND ----------

main_soldTo = spark.sql("""
  SELECT
    CAST(NULL AS STRING) AS createdBy,
    CAST(o.Order_included_date AS TIMESTAMP) AS createdOn,
    CAST(NULL AS STRING) AS modifiedBy,
    CAST(NULL AS TIMESTAMP) AS modifiedOn,
    CURRENT_TIMESTAMP() AS insertedOn,
    CURRENT_TIMESTAMP() AS updatedOn, 
    CONCAT(c.Company, '-', c.Customer_id) AS accountId,
    CONCAT(c.Company, '-', c.Customer_id) AS accountNumber,
    CONCAT('SOLD_TO-', c.Company, '-' , c.Customer_id) AS addressId,     
    c.Customer_address AS addressLine1,
    CAST(NULL AS STRING) AS addressLine2,
    CAST(NULL AS STRING) AS addressLine3,
    CAST(NULL AS STRING) AS addressLine4,
    c.Customer_city AS city,
    'BR' AS country,
    'BR' AS countryCode,
    NULL AS county,
    CAST(NULL AS STRING) AS emailAddress,
    CAST(NULL AS STRING) AS indiaIrpCustomerType,
    NULL AS mdmId,
    c.Customer_name AS name,
    CAST(NULL AS STRING) AS partySiteNumber,
    CAST(NULL AS STRING) AS phoneNumber,
    c.Customer_postal_cod AS postalCode,
    NULL AS primaryFlag,
    'LA' siteCategory,
    CAST(NULL AS STRING) AS siteUseId,
    'SOLD_TO-' AS siteUseCode,
    STATENAME_LKP.CVALUE AS stateName,
    c.Customer_state AS state
  FROM tb_pbi_customer c
  LEFT JOIN tb_pbi_orders_f o ON c.Customer_id = o.Customer_id and c.Company = o.Company
  LEFT JOIN STATENAME_LKP
       on STATENAME_LKP.KEY_VALUE = CONCAT('BR','-',trim(c.Customer_state) )
""")

display(main_soldTo)

# COMMAND ----------

main_payTo = spark.sql("""
  SELECT
    CAST(NULL AS STRING) AS createdBy,
    CAST(o.Order_included_date AS TIMESTAMP) AS createdOn,
    CAST(NULL AS STRING) AS modifiedBy,
    CAST(NULL AS TIMESTAMP) AS modifiedOn,
    CURRENT_TIMESTAMP() AS insertedOn,
    CURRENT_TIMESTAMP() AS updatedOn, 
    CONCAT(c.Company, '-', c.Customer_id) AS accountId,
    CONCAT(c.Company, '-', c.Customer_id) AS accountNumber,
    CONCAT('PAY_TO-', c.Company, '-' , c.Customer_id) AS addressId,       
    c.Customer_address AS addressLine1,
    CAST(NULL AS STRING) AS addressLine2,
    CAST(NULL AS STRING) AS addressLine3,
    CAST(NULL AS STRING) AS addressLine4,
    c.Customer_city AS city,
    'BR' AS country,
    'BR' AS countryCode,
    NULL AS county,
    CAST(NULL AS STRING) AS emailAddress,
    CAST(NULL AS STRING) AS indiaIrpCustomerType,
    NULL AS mdmId,
    c.Customer_name AS name,
    CAST(NULL AS STRING) AS partySiteNumber,
    CAST(NULL AS STRING) AS phoneNumber,
    c.Customer_postal_cod AS postalCode,
    NULL AS primaryFlag,
    'LA' siteCategory,
    CAST(NULL AS STRING) AS siteUseId,
    'PAY_TO-' AS siteUseCode,
    STATENAME_LKP.CVALUE AS stateName,
    c.Customer_state AS state
  FROM tb_pbi_customer c
  LEFT JOIN tb_pbi_orders_f o ON c.Customer_id = o.Customer_id and c.Company = o.Company
  LEFT JOIN STATENAME_LKP
       on STATENAME_LKP.KEY_VALUE = CONCAT('BR','-',trim(c.Customer_state) )
""")

display(main_payTo)

# COMMAND ----------

main = main_billTo.unionAll(main_shipTo).unionAll(main_soldTo).unionAll(main_payTo)

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

main = remove_duplicate_rows(df = main, 
                             key_columns = ['addressId'], 
                             tableName = table_name, 
                             sourceName = source_name, 
                             notebookName = NOTEBOOK_NAME, 
                             notebookPath = NOTEBOOK_PATH)

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .transform(tg_default(source_name))
  .transform(tg_core_customer_location())
  .transform(apply_schema(schema))
  .transform(attach_unknown_record)
  .select(columns)
  .transform(sort_columns)
)

main_f.cache()
main_f.display()

# COMMAND ----------

# PERSIST DATA
options = {'target_storage': target_storage, 'target_container': target_container}
merge_to_delta(main_f, table_name, target_folder, overwrite, options = options)

# COMMAND ----------

full_billto_key = spark.sql("""
Select
CONCAT('BILL_TO-', c.Company, '-' , c.Customer_id) AS addressId
 FROM tot.tb_pbi_customer c
""")

# COMMAND ----------

full_shipto_key = spark.sql("""
Select
CONCAT('SHIP_TO-', c.Company, '-' , c.Customer_delivery) AS addressId
 FROM tot.tb_pbi_customer c
""")

# COMMAND ----------

full_soldto_key = spark.sql("""
Select
CONCAT('SOLD_TO-', c.Company, '-' , c.Customer_id) AS addressId
 FROM tot.tb_pbi_customer c
""")

# COMMAND ----------

full_payto_key = spark.sql("""
Select
CONCAT('PAY_TO-', c.Company, '-' , c.Customer_id) AS addressId  
 FROM tot.tb_pbi_customer c
""")

# COMMAND ----------

# HANDLE DELETE
full_keys_f = (
  full_billto_key
  .unionAll(full_shipto_key)
  .unionAll(full_soldto_key)
  .unionAll(full_payto_key)
  .transform(attach_source_column(source = source_name))
  .transform(attach_primary_key('addressId,_SOURCE', 'edm.customer_location'))
  .select('_ID')
  .transform(add_unknown_ID())
)
  
apply_soft_delete(full_keys_f, table_name, key_columns = '_ID')

# COMMAND ----------

# UPDATE FK
update_foreign_key(table_name, 'createdBy,_SOURCE', 'createdBy_ID', 'edm.user')
update_foreign_key(table_name, 'modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user')
update_foreign_key(table_name, 'accountId,_SOURCE', 'account_ID', 'edm.account')

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not test_run:
  cutoff_value = get_incr_col_max_value(main_inc)
  update_cutoff_value(cutoff_value, table_name, 'tot.tb_pbi_customer')
  update_run_datetime(run_datetime, table_name, 'tot.tb_pbi_customer')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core
