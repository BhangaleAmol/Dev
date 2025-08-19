# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.account

# COMMAND ----------

# LOAD DATASETS
vw_customers = load_full_dataset('col.vw_customers')
vw_customers.createOrReplaceTempView('vw_customers')

# COMMAND ----------

# SAMPLING
if sampling:
  vw_customers = vw_customers.limit(10)
  vw_customers.createOrReplaceTempView('vw_customers')

# COMMAND ----------

# VALIDATE NK
valid_count_rows(vw_customers, 'Company, Customer_id')

# COMMAND ----------

forecast_Grp_LKP = spark.sql("""
select CVALUE,KEY_VALUE from smartsheets.edm_control_table where table_id = 'COLUMBIA_FORECAST_GROUPS' 
""")
forecast_Grp_LKP.createOrReplaceTempView("forecast_Grp_LKP")

# COMMAND ----------

main = spark.sql("""
SELECT
    CAST(NULL AS STRING) 						AS createdBy,
    CAST(NULL AS TIMESTAMP) 	                    AS createdOn,
    CAST(NULL AS STRING) 						AS modifiedBy,
    CAST(NULL AS TIMESTAMP) 					AS modifiedOn,
    CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss.SSS') AS TIMESTAMP) 						AS insertedOn,
    CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss.SSS') AS TIMESTAMP) 						AS updatedOn,
    CONCAT(c.Company, '-', c.Customer_id) 		AS accountId,
    CAST(NULL AS STRING)                        AS accountGroup,
    c.Customer_id 						        AS accountNumber,
    CAST(NULL AS STRING)   						AS accountStatus,
    CAST(NULL AS STRING)   						AS accountType,
    c.Customer_address  						AS address1Line1,
    CAST(NULL AS STRING) 						AS address1Line2,
    CAST(NULL AS STRING) 						AS address1Line3,
    c.Customer_city     						AS address1City,
    CASE 
      WHEN length(c.Country) = 0 THEN 'COLOMBIA'
     ELSE c.Country
    END                                         AS address1Country,
    c.Customer_postal_cod						AS address1PostalCode,
    c.Customer_state     						AS address1State,
    c.Customer_business_group				    AS businessGroupId,
    CAST(NULL AS STRING)   						AS businessType,
    '001'               						AS client,
    CAST(NULL AS STRING) 						AS customerDivision,
    CAST(NULL AS STRING)                        AS customerSegmentation,
    CAST(NULL AS STRING)                        AS customerTier,
    case 
      when c.Customer_type = '' 
        then 'External' 
      else c.Customer_type 
    end                                         AS customerType,
    CAST(NULL AS STRING)                        AS ebsAccountNumber,
    CAST(NULL AS boolean)                       AS eCommerceFlag,
    CAST(NULL AS STRING)                        AS erpId,
    NVL(forecast_Grp_LKP.CVALUE,'Other')                                     AS forecastGroup,
    'I'                                         AS gbu,
    CAST(NULL AS STRING)   						AS globalRegion,
    CAST(NULL AS STRING)                        AS industry,
    true                                        AS isActive,
    false                                       AS isDeleted,
    CAST(NULL AS STRING)                        AS kingdeeId,
    CAST(NULL AS STRING)   						AS locationType,
    CAST(NULL AS STRING)   						AS marketType,
    c.Customer_name                     		AS name,
    CAST(NULL AS boolean)   					AS nationalAccount,
    CAST(NULL AS int)   						AS numberOperatingRooms,
    CAST(NULL AS STRING)   						AS organization,
    CAST(NULL AS STRING)   						AS ownerId,
    CAST(NULL AS STRING)   						AS ownerRegion,
    CAST(NULL AS STRING) 						AS parentAccountId,
    CAST(NULL AS STRING) 						AS partyId,
    CAST(NULL AS STRING) 						AS priceListId,
    CAST(NULL AS STRING)   						AS region,
    CAST(NULL AS STRING) 						AS registrationId, 
    CAST(NULL AS STRING)                        AS sapId,
    CAST(NULL AS STRING)						AS salesOrganizationID,
    CAST(NULL AS boolean)  						AS syncWithGuardian,
    CAST(NULL AS STRING) 						AS subIndustry,
    CAST(NULL AS STRING)   						AS subVertical,
    CAST(NULL AS STRING)						AS territoryId,
    CAST(NULL AS STRING)   						AS topXAccountGuardian,
    CAST(NULL AS STRING)   						AS topXTargetGuardian,
    CAST(NULL AS STRING) 						AS vertical
  FROM vw_customers c
    LEFT JOIN forecast_Grp_LKP 
      ON forecast_Grp_LKP.KEY_VALUE = c.Customer_id 
""")
main.cache()
main.count()
main.display()

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

main = remove_duplicate_rows(df = main, 
                             key_columns = ['accountId'], 
                             tableName = table_name, 
                             sourceName = source_name, 
                             notebookName = NOTEBOOK_NAME, 
                             notebookPath = NOTEBOOK_PATH)

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
  spark.table('col.vw_customers')
  .filter('_DELETED IS FALSE')
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
  cutoff_value = get_incr_col_max_value(vw_customers)
  update_cutoff_value(cutoff_value, table_name, 'col.vw_customers')
  update_run_datetime(run_datetime, table_name, 'col.vw_customers')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core
