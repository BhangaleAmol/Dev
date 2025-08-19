# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.account

# COMMAND ----------

# LOAD DATASETS
tb_pbi_customer = load_full_dataset('tot.tb_pbi_customer')
tb_pbi_customer.createOrReplaceTempView('tb_pbi_customer')

# COMMAND ----------

# SAMPLING
if sampling:
  tb_pbi_customer = tb_pbi_customer.limit(10)
  tb_pbi_customer.createOrReplaceTempView('tb_pbi_customer')

# COMMAND ----------

tb_pbi_customer_2 = (
  tb_pbi_customer
  .select('Company', 'Customer_id', 'Customer_name', 'Customer_business_group', 
          'Customer_TSM_cod', 'customer_address', 'customer_city', 'customer_state', 
          'Country', 'customer_forecast_code', 'Customer_Division', 'Tier')
  .distinct()
)
tb_pbi_customer_2.createOrReplaceTempView('tb_pbi_customer_2')

# COMMAND ----------

# VALIDATE NK
valid_count_rows(tb_pbi_customer_2, 'Company, Customer_id')

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

main = spark.sql("""
SELECT
    CAST(NULL AS STRING) AS createdBy,
    CAST(o.Order_included_date AS TIMESTAMP) AS createdOn,
    CAST(NULL AS STRING) AS modifiedBy,
    CAST(NULL AS TIMESTAMP) AS modifiedOn,
    CURRENT_TIMESTAMP() AS insertedOn,
    CURRENT_TIMESTAMP() AS updatedOn,
    CONCAT(c.Company, '-', c.Customer_id) AS accountId,
    CAST(NULL AS STRING) AS accountGroup,
    c.Customer_id AS accountNumber,
    CAST(NULL AS STRING) AS accountStatus,
    CAST(NULL AS STRING) AS accountType,
    customer_address AS address1Line1,
    CAST(NULL AS STRING) AS address1Line2,
    CAST(NULL AS STRING) AS address1Line3,
    customer_city AS address1City,
    COALESCE(country,'Brazil') AS address1Country,
    CAST(NULL AS STRING) AS address1PostalCode,
    customer_state AS address1State,
    --CONCAT(c.Company, '-', c.Customer_business_group) AS businessGroupId,
    case
    when c.Customer_business_group ='' 
          then  NULL
    else 'BusinessGroup' || '-' || c.Company || '-' || c.Customer_business_group 
    end AS businessGroupId,
    CAST(NULL AS STRING) AS businessType,
    '001' AS client,
    Customer_Division AS customerDivision,
    CAST(NULL AS STRING) AS customerSegmentation,
    Tier AS customerTier,
    'External' AS customerType,
    CAST(NULL AS STRING) AS ebsAccountNumber,
    False AS eCommerceFlag,
    CAST(NULL AS STRING) AS  erpId,
    Customer_Forecast_Code AS forecastGroup,
    case 
      when Customer_Division= 'I'
        then 'INDUSTRIAL'
      when Customer_Division= 'M'
        then 'MEDICAL'
      else Customer_Division
    end AS gbu,
    CAST(NULL AS STRING) AS globalRegion,
    CAST(NULL AS STRING) AS industry,
    true AS isActive,
    false AS isDeleted,
    CAST(NULL AS STRING) AS  kingdeeId,
    CAST(NULL AS STRING) AS locationType,
    CAST(NULL AS STRING) AS marketType, 
    Customer_name AS name,
    CAST(NULL AS boolean) AS nationalAccount,
    CAST(NULL AS int) AS numberOperatingRooms,
    CAST(NULL AS STRING) AS organization,
    CAST(NULL AS STRING) AS ownerId,
    CAST(NULL AS STRING) AS ownerRegion, 
    CAST(NULL AS STRING) AS parentAccountId,
    CAST(NULL AS STRING) AS partyId,
    CAST(NULL AS STRING) AS priceListId,
    CAST(NULL AS STRING) AS region,
    CAST(NULL AS STRING) AS registrationId,
    CAST(NULL AS STRING) AS sapId,
    Customer_TSM_cod AS salesOrganizationID,
    CAST(NULL AS boolean) AS syncWithGuardian,
    CAST(NULL AS STRING) AS subIndustry,
    CAST(NULL AS STRING) AS subVertical,
    Customer_TSM_cod AS territoryId,
    CAST(NULL AS STRING) AS topXAccountGuardian,
    CAST(NULL AS STRING) AS topXTargetGuardian,
    CAST(NULL AS STRING) AS vertical
  FROM tb_pbi_customer_2 c
  LEFT JOIN tb_pbi_orders_f o ON c.Customer_id = o.Customer_id and c.Company = o.Company
""")
main.cache()
display(main)

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

main = remove_duplicate_rows(df = main, key_columns = ['accountId'], tableName = table_name, sourceName = source_name, notebookName = NOTEBOOK_NAME, notebookPath = NOTEBOOK_PATH)

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .transform(tg_default(source_name))
  .transform(tg_core_account())
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
full_keys_f = (
  spark.table('tot.tb_pbi_customer')
  .filter('_DELETED IS FALSE')
  .select('Company', 'Customer_id', 'Customer_name', 'Customer_business_group', 
          'Customer_TSM_cod', 'customer_address', 'customer_city', 'customer_state', 'Country')
  .distinct()
  .selectExpr("CONCAT(Company, '-', Customer_id) AS accountId")
  .transform(attach_source_column(source = source_name))
  .transform(attach_primary_key('accountId,_SOURCE', 'edm.account'))
  .select('_ID')
  .transform(add_unknown_ID())
)

apply_soft_delete(full_keys_f, table_name, key_columns = '_ID')

# COMMAND ----------

# UPDATE FK
update_foreign_key(table_name, 'createdBy,_SOURCE', 'createdBy_ID', 'edm.user')
update_foreign_key(table_name, 'modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user')


# COMMAND ----------

# UPDATE CUTOFF VALUE
if not test_run:
  cutoff_value = get_incr_col_max_value(tb_pbi_customer)
  update_cutoff_value(cutoff_value, table_name, 'tot.tb_pbi_customer')
  update_run_datetime(run_datetime, table_name, 'tot.tb_pbi_customer')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core
