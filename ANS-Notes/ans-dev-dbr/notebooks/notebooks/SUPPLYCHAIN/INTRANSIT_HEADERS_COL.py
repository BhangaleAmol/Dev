# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_supplychain

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_supplychain.intransit_headers

# COMMAND ----------

# LOAD DATASETS
main_inc = load_full_dataset('col.vw_mle_in_transit')

# COMMAND ----------

# SAMPLING
if sampling:
  main_inc = main_inc.limit(10)

# COMMAND ----------

# VIEWS
main_inc.createOrReplaceTempView('main_inc')

# COMMAND ----------

main = spark.sql("""
SELECT   
  DISTINCT
  0                   				AS createdBy,
  CURRENT_TIMESTAMP() 				AS createdOn,
  0                   				AS modifiedBy,
  CAST(NULL AS TIMESTAMP)			AS modifiedOn,
  CURRENT_TIMESTAMP() 				AS insertedOn,
  CURRENT_TIMESTAMP() 				AS updatedOn,   
  'COP' 							AS baseCurrencyId,
  '' 								AS cancelledFlag,
  CAST(NULL AS DATE)  				AS closedDate,
  'COP' 							AS currency,
  CAST(NULL AS DECIMAL(22,7))  		AS exchangeRate,
  CAST(NULL AS TIMESTAMP) 			AS exchangeRateDate,
  CAST(NULL AS DECIMAL(22,7)) 		AS exchangeRateUsd,
  CAST(NULL AS DECIMAL(22,7))  		AS inTransitTime,
  '' 								AS inventoryWarehouseId,
  PO_NUM 							AS orderNumber,
  '' 								AS orderStatus,
  case 
	when left(CHAN_ID,3) = '819' 
	then '5400' 
  end 								AS owningBusinessUnitId,
  PO_NUM 							AS purchaseOrderId,
  USERSTRING__2 					AS supplierId,
  '' 								AS supplierSiteId
FROM main_inc
Where USERSTRING_0 = 'IN TRANSIT'
""")
main.createOrReplaceTempView("main")
main.count()
display(main)

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

main = remove_duplicate_rows(df = main, 
                             key_columns = ['purchaseOrderId'], 
                             tableName = table_name, 
                             sourceName = source_name, 
                             notebookName = NOTEBOOK_NAME, 
                             notebookPath = NOTEBOOK_PATH)

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .transform(tg_default(source_name))
  .transform(tg_supplychain_intransit_headers())
  .transform(attach_surrogate_key(columns = 'purchaseOrderId,_SOURCE'))
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
    SELECT
          DISTINCT 
          PO_NUM AS purchaseOrderId 
    FROM col.vw_mle_in_transit
    Where USERSTRING_0 = 'IN TRANSIT'
  """)
  .transform(attach_source_column(source = source_name))
  .transform(attach_surrogate_key(columns = 'purchaseOrderId,_SOURCE'))
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
  update_cutoff_value(cutoff_value, table_name, 'col.vw_mle_in_transit')
  update_run_datetime(run_datetime, table_name, 'col.vw_mle_in_transit')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_supplychain
