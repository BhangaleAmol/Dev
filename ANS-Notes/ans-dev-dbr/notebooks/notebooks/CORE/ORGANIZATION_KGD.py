# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.organization

# COMMAND ----------

# LOAD DATASETS
vw_company = load_full_dataset('kgd.dbo_tembo_seorderentry')
vw_warehouse = load_full_dataset('kgd.dbo_tembo_seorderentry')

# COMMAND ----------

# SAMPLING
if sampling:
  vw_company = vw_company.limit(10)
  vw_warehouse = vw_warehouse.limit(10)

# COMMAND ----------

# VIEWS
vw_company.createOrReplaceTempView('vw_company')
vw_warehouse.createOrReplaceTempView('vw_warehouse')

# COMMAND ----------

vw_warehouse_f = spark.sql("""
select distinct
   '' createdBy,
   cast(NULL AS timestamp) AS createdOn,
   '' modifiedBy,
   cast(NULL AS timestamp) AS modifiedOn,
   CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss.SSS') AS TIMESTAMP) insertedOn,
   CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss.SSS') AS TIMESTAMP) updatedOn,
   CAST(NULL AS STRING) AS addressLine1,
   CAST(NULL AS STRING) AS addressLine2,
   CAST(NULL AS STRING) AS addressLine3,
   CAST(NULL AS STRING) AS city,
   '' AS commonName,
   'DC' || WarehouseCode AS commonOrganizationCode,
   'CHN' AS country,
   'CHN' AS countryCode,
   'RMB' AS currency,
   TRUE AS isActive,
   'Shangai Warehouse'  AS name,
   WarehouseCode AS organizationCode,
   WarehouseCode AS organizationId,
   'INV' AS organizationType,
   CAST(NULL AS STRING) AS postalCode,
   CAST(NULL AS STRING) AS region,
   CAST(NULL AS STRING) AS subRegion,
   CAST(NULL AS STRING) AS state
from
  vw_warehouse WAREHOUSE 
where NOT(_DELETED)
""")
vw_warehouse_f.cache()
display(vw_warehouse_f)

# COMMAND ----------

vw_company_f = spark.sql("""
SELECT DISTINCT
    '' createdBy,
	'' createdOn,
	'' modifiedBy,
	'' modifiedOn,
	CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss.SSS') AS TIMESTAMP) insertedOn,
	CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss.SSS') AS TIMESTAMP) updatedOn,
    CAST(NULL AS STRING) AS addressLine1,
    CAST(NULL AS STRING) AS addressLine2,
    CAST(NULL AS STRING) AS addressLine3,
    CAST(NULL AS STRING) AS city,
    '' AS commonName,
	'' AS commonOrganizationCode,
    'CHN' AS country,
    'CHN' AS countryCode,
    Null currency,
	TRUE		 isActive,
    'Ansell Shanghai' name,
	'AS' AS organizationCode,
    'AS' organizationId,
	'OPERATING_UNIT' organizationType,
    CAST(NULL AS STRING) AS postalCode,
    CAST(NULL AS STRING) AS region,
    CAST(NULL AS STRING) AS subRegion,
    CAST(NULL AS STRING) AS state
FROM vw_company COMPANY
WHERE NOT(_DELETED)
""")
vw_company_f.cache()
display(vw_company_f)

# COMMAND ----------

main = vw_warehouse_f.unionAll(vw_company_f)
main.cache()

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

main = remove_duplicate_rows(df = main, 
                             key_columns = ['organizationId'], 
                             tableName = table_name, 
                             sourceName = source_name, 
                             notebookName = NOTEBOOK_NAME, 
                             notebookPath = NOTEBOOK_PATH)

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .transform(parse_timestamp(['createdOn','modifiedOn'], expected_format = 'MM/dd/yy')) 
  .transform(tg_default(source_name))
  .transform(tg_core_organization())
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
full_company_keys = spark.sql("""SELECT DISTINCT WarehouseCode AS organizationId
from
  vw_warehouse WAREHOUSE
WHERE NOT(_DELETED)
""")
  
full_warehouse_keys = spark.sql("""
  select distinct
    'AS' organizationId	
FROM vw_company COMPANY
WHERE NOT(_DELETED)
""")

full_keys_f = (
  full_company_keys
  .unionAll(full_warehouse_keys)
  .transform(attach_source_column(source = source_name))
  .transform(attach_surrogate_key(columns = 'organizationId,_SOURCE'))
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
  cutoff_value = get_incr_col_max_value(vw_company)
  update_cutoff_value(cutoff_value, table_name, 'kgd.dbo_tembo_seorderentry')
  update_run_datetime(run_datetime, table_name, 'kgd.dbo_tembo_seorderentry')

  cutoff_value = get_incr_col_max_value(vw_warehouse)
  update_cutoff_value(cutoff_value, table_name, 'kgd.dbo_tembo_seorderentry')
  update_run_datetime(run_datetime, table_name, 'kgd.dbo_tembo_seorderentry')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core
