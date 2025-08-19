# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# LOAD DATASETS
if incremental:
  
  cutoff_value = get_cutoff_value(table_name, 's_core.supplier_account_ebs', prune_days) 
  supplier_account_ebs = (
                        spark.table('s_core.supplier_account_ebs')
                      .filter(f"_MODIFIED > '{cutoff_value}'")
                    )
  
  cutoff_value = get_cutoff_value(table_name, 's_core.supplier_account_sap', prune_days) 
  supplier_account_sap = (
                        spark.table('s_core.supplier_account_sap')
                      .filter(f"_MODIFIED > '{cutoff_value}'")
                    )
  
  
else:
  supplier_account_ebs = spark.table('s_core.supplier_account_ebs')
  supplier_account_sap = spark.table('s_core.supplier_account_sap')
  

# COMMAND ----------

supplier_account_ebs = supplier_account_ebs.filter("_ID != '0'")
supplier_account_sap = supplier_account_sap.filter("_ID != '0'")

# COMMAND ----------

# SAMPLING
if sampling:
  supplier_account_ebs = supplier_account_ebs.limit(10)
  supplier_account_sap = supplier_account_sap.limit(10)


# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.supplier_account

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

supplier_account_ebs_f = (
  supplier_account_ebs
  .select(columns)
  .transform(apply_schema(schema))
)

supplier_account_sap_f =  (
  supplier_account_sap
  .select(columns)
  .transform(apply_schema(schema))
)

# COMMAND ----------

main_f = (
  supplier_account_ebs_f
  .union(supplier_account_sap_f)
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
  cutoff_value = get_incr_col_max_value(supplier_account_ebs, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.supplier_account_ebs')
  update_run_datetime(run_datetime, table_name, 's_core.supplier_account_ebs')
  
  cutoff_value = get_incr_col_max_value(supplier_account_sap, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.supplier_account_sap')
  update_run_datetime(run_datetime, table_name, 's_core.supplier_account_sap')  

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core
