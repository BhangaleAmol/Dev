# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.organization

# COMMAND ----------

# LOAD DATASETS
main_inc = load_full_dataset('pdh.Org_Assignments')

# COMMAND ----------

# SAMPLING
if sampling:
  main_inc = main_inc.limit(10)

# COMMAND ----------

# VIEWS
main_inc.createOrReplaceTempView('main_inc')

# COMMAND ----------

main_inv = spark.sql("""
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
    CAST(NULL AS STRING) AS country,
    CAST(NULL AS STRING) AS countryCode,
    null currency,
	TRUE		 isActive,
    '' AS name,
    ORG_WAREHOUSE organizationCode,
    ORG_WAREHOUSE organizationId,
    'INV' organizationType,
    CAST(NULL AS STRING) AS postalCode,
    region_dc_master.region,
    region_dc_master.subRegion,
    CAST(NULL AS STRING) AS state
FROM main_inc WAREHOUSE
  LEFT join qv.region_dc_master
      on region_dc_master.ERPWHCode = ORG_WAREHOUSE
where WAREHOUSE.ORG_WAREHOUSE is not null 
""")

main_inv.cache()
display(main_inv)

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

main_inv = remove_duplicate_rows(df = main_inv, 
                             key_columns = ['organizationId'], 
                             tableName = table_name, 
                             sourceName = source_name, 
                             notebookName = NOTEBOOK_NAME, 
                             notebookPath = NOTEBOOK_PATH)

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main_inv
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
full_ai_keys_f = (
  spark.table('pdh.Org_Assignments')
  .filter('_DELETED IS FALSE')
  .filter('ORG_WAREHOUSE IS NOT NULL')
  .selectExpr("ORG_WAREHOUSE AS organizationId")
  .select('organizationId')
)

full_keys_f = (
  full_ai_keys_f
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
  cutoff_value = get_incr_col_max_value(main_inc)
  update_cutoff_value(cutoff_value, table_name, 'pdh.Org_Assignments')
  update_run_datetime(run_datetime, table_name, 'pdh.Org_Assignments')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core
