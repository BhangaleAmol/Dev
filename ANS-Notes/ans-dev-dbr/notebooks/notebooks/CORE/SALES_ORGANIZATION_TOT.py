# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.sales_organization

# COMMAND ----------

# LOAD DATASETS
main_inc = load_full_dataset('tot.tb_pbi_tsm')

# COMMAND ----------

# SAMPLING
if sampling:
  main_inc = main_inc.limit(10)

# COMMAND ----------

# VIEWS
main_inc.createOrReplaceTempView('tb_pbi_tsm')

# COMMAND ----------

main = spark.sql("""
SELECT '' createdBy,
	CAST(NULL AS TIMESTAMP) createdOn,
	'' modifiedBy,
	CAST(NULL AS TIMESTAMP) modifiedOn,
	CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss.SSS') AS TIMESTAMP) insertedOn,
	CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss.SSS') AS TIMESTAMP) updatedOn,
    o.TSM_cod salesOrganizationID,
    o.TSM_cod territoryID,
	o.TSM_name		 name
FROM tb_pbi_tsm o
""")

main.cache()
display(main)

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

main = remove_duplicate_rows(df = main, 
                             key_columns = ['salesOrganizationID'], 
                             tableName = table_name, 
                             sourceName = source_name, 
                             notebookName = NOTEBOOK_NAME, 
                             notebookPath = NOTEBOOK_PATH)

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .transform(tg_default(source_name))
  .transform(tg_core_sales_organization())
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
  spark.table('tot.tb_pbi_tsm')
  .filter('_DELETED IS FALSE')
  .selectExpr("TSM_cod salesOrganizationID")
  .transform(attach_source_column(source = source_name))
  .transform(attach_primary_key('salesOrganizationID,_SOURCE', 'edm.salesorganization'))
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
  update_cutoff_value(cutoff_value, table_name, 'tot.tb_pbi_tsm')
  update_run_datetime(run_datetime, table_name, 'tot.tb_pbi_tsm')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core
