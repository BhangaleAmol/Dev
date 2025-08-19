# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.customer_sales_organization

# COMMAND ----------

# LOAD DATASETS
main_inc = load_full_dataset('tot.tb_pbi_rsm')

# COMMAND ----------

# SAMPLING
if sampling:
  main_inc = main_inc.limit(10)

# COMMAND ----------

# VIEWS
main_inc.createOrReplaceTempView('main_inc')

# COMMAND ----------

main = spark.sql("""
  SELECT distinct
    NULL AS createdBy,
    NULL AS createdOn,
    NULL AS modifiedBy,
    NULL AS modifiedOn,
    CURRENT_TIMESTAMP() AS insertedOn,
    CURRENT_TIMESTAMP() AS updatedOn,
    CONCAT(Company, '-' , CustomerNumber) as accountId,
    CONCAT('BusinessGroup', '-' , Company , '-' , Business_Group) as  businessGroupId,
    Percent_Customer as percentAllocated,
    RSM as regionManager,
    TM as territoryManager
  FROM main_inc cc
  Where Company = '5400'
""")

main.cache()
main.display()

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

main = remove_duplicate_rows(df = main, 
                             key_columns = ['accountId', 'regionManager', 'territoryManager'], 
                             tableName = table_name, 
                             sourceName = source_name, 
                             notebookName = NOTEBOOK_NAME, 
                             notebookPath = NOTEBOOK_PATH)

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .transform(tg_default(source_name))
  .transform(tg_core_customer_sales_organization())
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
  spark.sql("""
    SELECT DISTINCT
      CONCAT(Company, '-' , CustomerNumber) as accountId,
      RSM as regionManager,
      TM as territoryManager
  FROM tot.tb_pbi_rsm
  Where Company = '5400'
  """)
  .transform(attach_source_column(source = source_name))
  .transform(attach_surrogate_key(columns = 'accountId, regionManager, territoryManager,_SOURCE'))
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
  update_cutoff_value(cutoff_value, table_name, 'tot.tb_pbi_rsm')
  update_run_datetime(run_datetime, table_name, 'tot.tb_pbi_rsm')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core
