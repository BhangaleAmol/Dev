# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.sales_organization

# COMMAND ----------

# LOAD DATASETS
main_inc = load_full_dataset('sf.account')

# COMMAND ----------

# SAMPLING
if sampling:
  main_inc = main_inc.limit(10)

# COMMAND ----------

# VIEWS
main_inc.createOrReplaceTempView('account')

# COMMAND ----------

main = spark.sql("""
SELECT DISTINCT 
    '' createdBy,
	CAST(NULL AS TIMESTAMP) AS createdOn,
	'' modifiedBy,
	CAST(NULL AS TIMESTAMP) AS modifiedOn,
	CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss.SSS') AS TIMESTAMP) insertedOn,
	CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss.SSS') AS TIMESTAMP) updatedOn,
    o.Territory__c territoryID,
	o.Territory__c name
FROM account o
Where o.Territory__c is not null
""")

main.cache()
display(main)

# COMMAND ----------

main = remove_duplicate_rows(df = main, 
                             key_columns = ['territoryID'], 
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
add_unknown_record(main_f, table_name)

# COMMAND ----------

# HANDLE DELETE
full_keys_f = (
  spark.table('sf.account')
  .filter('_DELETED IS FALSE')
  .selectExpr("Territory__c territoryId")
  .transform(attach_source_column(source = source_name))
  .transform(attach_surrogate_key(columns = 'territoryId,_SOURCE'))
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
  update_cutoff_value(cutoff_value, table_name, 'sf.account')
  update_run_datetime(run_datetime, table_name, 'sf.account')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core
