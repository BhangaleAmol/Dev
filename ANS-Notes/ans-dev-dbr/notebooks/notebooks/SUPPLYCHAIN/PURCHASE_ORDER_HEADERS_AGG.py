# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_supplychain

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 's_supplychain.purchase_order_headers_ebs', prune_days)
  purchase_order_headers_ebs = ( 
                                spark.table('s_supplychain.purchase_order_headers_ebs')
                                .filter(f"_MODIFIED > '{cutoff_value}'")
                              ) 
  
else:
  purchase_order_headers_ebs   = spark.table('s_supplychain.purchase_order_headers_ebs')

# COMMAND ----------

 purchase_order_headers_ebs  = purchase_order_headers_ebs.filter("_ID != '0'") 

# COMMAND ----------

# SAMPLING
if sampling:
  purchase_order_headers_ebs  = purchase_order_headers_ebs.limit(10)

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_supplychain.purchase_order_headers

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

purchase_order_headers_ebs_f = (
  purchase_order_headers_ebs
  .select(columns)
  .transform(apply_schema(schema))
)

# COMMAND ----------

main_f = (
  purchase_order_headers_ebs_f 
  .transform(attach_unknown_record)
  .select(columns)
  .transform(sort_columns)
)

# COMMAND ----------

# PERSIST DATA
options = {'target_storage': target_storage, 'target_container': target_container}
merge_to_delta(main_f, table_name, target_folder, overwrite, options = options)

# COMMAND ----------

# UPDATE FK
update_foreign_key(table_name, 'createdBy,_SOURCE', 'createdBy_ID', 'edm.user')
update_foreign_key(table_name, 'modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user')

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not (test_run or sampling):
  cutoff_value = get_incr_col_max_value(purchase_order_headers_ebs, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_supplychain.purchase_order_headers_ebs')
  update_run_datetime(run_datetime, table_name, 's_supplychain.purchase_order_headers_ebs')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_supplychain
