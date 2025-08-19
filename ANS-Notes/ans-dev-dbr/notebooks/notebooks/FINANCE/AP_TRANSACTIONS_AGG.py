# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_finance

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_finance.ap_transactions

# COMMAND ----------

# LOAD DATASETS
if incremental:
  
  cutoff_value = get_cutoff_value(table_name, 's_finance.ap_transactions_ebs', prune_days) 
  ap_transactions_ebs =  ( 
                            spark.table('s_finance.ap_transactions_ebs')
                            .filter(f"_MODIFIED > '{cutoff_value}'")
                          )

  
else:
  ap_transactions_ebs = spark.table('s_finance.ap_transactions_ebs').filter("_ID != '0'")
 

# COMMAND ----------

if sampling:
  ap_transactions_ebs = ap_transactions_ebs.limit(10)

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

ap_transactions_ebs_f = (
  ap_transactions_ebs
  .select(columns)
  .transform(apply_schema(schema))
)

# COMMAND ----------

main_f = (
          ap_transactions_ebs_f
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
  
  cutoff_value = get_incr_col_max_value(ap_transactions_ebs, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_finance.ap_transactions_ebs')
  update_run_datetime(run_datetime, table_name, 's_finance.ap_transactions_ebs')
  

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_finance
