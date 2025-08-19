# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.customer_location

# COMMAND ----------

# LOAD DATASETS
main_inc = load_full_dataset('kgd.dbo_tembo_customer')

# COMMAND ----------

# SAMPLING
if sampling:
  main_inc = main_inc.limit(10)

# COMMAND ----------

# VIEWS
main_inc.createOrReplaceTempView('dbo_tembo_customer')

# COMMAND ----------

main_ship_to = spark.sql("""
  SELECT
    c.CreateBy AS createdBy,
    CAST(c.CreateDate AS TIMESTAMP) AS createdOn,
    CAST(NULL AS STRING) AS modifiedBy,
    CAST(c.LastModifyDate AS TIMESTAMP) AS modifiedOn,
    CURRENT_TIMESTAMP() AS insertedOn,
    CURRENT_TIMESTAMP() AS updatedOn, 
    c.Cust_ID AS accountId,
    c.Cust_ID AS accountNumber,
    CONCAT('SHIP_TO','-',c.Cust_ID) AS addressId,  
    CONCAT(c.ShipToAddress, ' ' ,c.BillAddress) AS addressLine1,
    CAST(NULL AS STRING) AS addressLine2,
    CAST(NULL AS STRING) AS addressLine3,
    CAST(NULL AS STRING) AS addressLine4,
    CAST(NULL AS STRING) AS city,
    'CN' AS country,
    'CN' AS countryCode,
    NULL AS county,
    CAST(NULL AS STRING) AS emailAddress,
    CAST(NULL AS STRING) AS indiaIrpCustomerType,
    NULL AS mdmId,
    c.CustomerName AS name,
    CAST(NULL AS STRING) AS partySiteNumber,
    CAST(NULL AS STRING) AS phoneNumber,
    c.PostalCode AS postalCode,
    NULL AS primaryFlag,
    NULL siteCategory,
    CAST(NULL AS STRING) AS siteUseId,
    'SHIP_TO' AS siteUseCode,  
    NULL AS stateName,
    NULL AS state
  FROM dbo_tembo_customer c
  WHERE NOT(_DELETED)
""")

main_ship_to.display()

# COMMAND ----------

main_bill_to = spark.sql("""
  SELECT
    c.CreateBy AS createdBy,
    CAST(c.CreateDate AS TIMESTAMP) AS createdOn,
    CAST(NULL AS STRING) AS modifiedBy,
    CAST(c.LastModifyDate AS TIMESTAMP) AS modifiedOn,
    CURRENT_TIMESTAMP() AS insertedOn,
    CURRENT_TIMESTAMP() AS updatedOn, 
    c.Cust_ID AS accountId,
    c.Cust_ID AS accountNumber,
    CONCAT('BILL_TO','-',c.Cust_ID) AS addressId,  
    CONCAT(c.ShipToAddress, " " ,c.BillAddress) AS addressLine1,
    CAST(NULL AS STRING) AS addressLine2,
    CAST(NULL AS STRING) AS addressLine3,
    CAST(NULL AS STRING) AS addressLine4,
    CAST(NULL AS STRING) AS city,
    'CN' AS country,
    'CN' AS countryCode,
    NULL AS county,
    CAST(NULL AS STRING) AS emailAddress,
    CAST(NULL AS STRING) AS indiaIrpCustomerType,
    NULL AS mdmId,
    c.CustomerName AS name,
    CAST(NULL AS STRING) AS partySiteNumber,
    CAST(NULL AS STRING) AS phoneNumber,
    c.PostalCode AS postalCode,
    NULL AS primaryFlag,
    NULL siteCategory,
    CAST(NULL AS STRING) AS siteUseId,
    'BILL_TO' AS siteUseCode,  
    NULL AS stateName,
    NULL AS state
  FROM dbo_tembo_customer c
  WHERE NOT(_DELETED)
""")

main_bill_to.display()

# COMMAND ----------

main_sold_to = spark.sql("""
  SELECT
    c.CreateBy AS createdBy,
    CAST(c.CreateDate AS TIMESTAMP) AS createdOn,
    CAST(NULL AS STRING) AS modifiedBy,
    CAST(c.LastModifyDate AS TIMESTAMP) AS modifiedOn,
    CURRENT_TIMESTAMP() AS insertedOn,
    CURRENT_TIMESTAMP() AS updatedOn, 
    c.Cust_ID AS accountId,
    c.Cust_ID AS accountNumber,
    CONCAT('SOLD_TO','-',c.Cust_ID) AS addressId,  
    CONCAT(c.ShipToAddress, " " ,c.BillAddress) AS addressLine1,
    CAST(NULL AS STRING) AS addressLine2,
    CAST(NULL AS STRING) AS addressLine3,
    CAST(NULL AS STRING) AS addressLine4,
    CAST(NULL AS STRING) AS city,
    'CN' AS country,
    'CN' AS countryCode,
    NULL AS county,
    CAST(NULL AS STRING) AS emailAddress,
    CAST(NULL AS STRING) AS indiaIrpCustomerType,
    NULL AS mdmId,
    c.CustomerName AS name,
    CAST(NULL AS STRING) AS partySiteNumber,
    CAST(NULL AS STRING) AS phoneNumber,
    c.PostalCode AS postalCode,
    NULL AS primaryFlag,
    NULL siteCategory,
    CAST(NULL AS STRING) AS siteUseId,
    'SOLD_TO' AS siteUseCode,  
    NULL AS stateName,
    NULL AS state
  FROM dbo_tembo_customer c
  WHERE NOT(_DELETED)
""")

main_sold_to.display()

# COMMAND ----------

main_pay_to = spark.sql("""
  SELECT
    c.CreateBy AS createdBy,
    CAST(c.CreateDate AS TIMESTAMP) AS createdOn,
    CAST(NULL AS STRING) AS modifiedBy,
    CAST(c.LastModifyDate AS TIMESTAMP) AS modifiedOn,
    CURRENT_TIMESTAMP() AS insertedOn,
    CURRENT_TIMESTAMP() AS updatedOn, 
    c.Cust_ID AS accountId,
    c.Cust_ID AS accountNumber,
    CONCAT('PAY_TO','-',c.Cust_ID) AS addressId,  
    CONCAT(c.ShipToAddress, " " ,c.BillAddress) AS addressLine1,
    CAST(NULL AS STRING) AS addressLine2,
    CAST(NULL AS STRING) AS addressLine3,
    CAST(NULL AS STRING) AS addressLine4,
    CAST(NULL AS STRING) AS city,
    'CN' AS country,
    'CN' AS countryCode,
    NULL AS county,
    CAST(NULL AS STRING) AS emailAddress,
    CAST(NULL AS STRING) AS indiaIrpCustomerType,
    NULL AS mdmId,
    c.CustomerName AS name,
    CAST(NULL AS STRING) AS partySiteNumber,
    CAST(NULL AS STRING) AS phoneNumber,
    c.PostalCode AS postalCode,
    NULL AS primaryFlag,
    NULL siteCategory,
    CAST(NULL AS STRING) AS siteUseId,
    'PAY_TO' AS siteUseCode,  
    NULL AS stateName,
    NULL AS state
  FROM dbo_tembo_customer c
  WHERE NOT(_DELETED)
""")

main_pay_to.display()

# COMMAND ----------

main = main_ship_to.unionAll(main_bill_to).unionAll(main_sold_to).unionAll(main_pay_to)

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

# HANDLE DELETE
full_keys_SHIP_TO_f = (
  spark.table('kgd.dbo_tembo_customer')
  .filter("NOT(_DELETED)")
  .selectExpr("'SHIP_TO-' || Cust_ID AS addressId")
  .select('addressId')
)

full_keys_BILL_TO_f = (
  spark.table('kgd.dbo_tembo_customer')
  .filter("NOT(_DELETED)")
  .selectExpr("'BILL_TO-' || Cust_ID AS addressId")
  .select('addressId')
)

full_keys_SOLD_TO_f = (
  spark.table('kgd.dbo_tembo_customer')
  .filter("NOT(_DELETED)")
  .selectExpr("'SOLD_TO-' || Cust_ID AS addressId")
  .select('addressId')
)

full_keys_PAY_TO_f = (
  spark.table('kgd.dbo_tembo_customer')
  .filter("NOT(_DELETED)")
  .selectExpr("'PAY_TO-' || Cust_ID AS addressId")
  .select('addressId')
)

full_keys_f = (
  full_keys_SHIP_TO_f
  .unionAll(full_keys_BILL_TO_f)
  .unionAll(full_keys_SOLD_TO_f)
  .unionAll(full_keys_PAY_TO_f)
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
  update_cutoff_value(cutoff_value, table_name, 'kgd.dbo_tembo_customer')
  update_run_datetime(run_datetime, table_name, 'kgd.dbo_tembo_customer')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core
