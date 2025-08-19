# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_finance

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_finance.gl_accounts

# COMMAND ----------

# LOAD DATASETS
if incremental:
  
  cutoff_value = get_cutoff_value(table_name, 's_finance.gl_accounts_ebs', prune_days) 
  gl_accounts_ebs = ( 
                      spark.table('s_finance.gl_accounts_ebs')
                      .filter(f"_MODIFIED > '{cutoff_value}'")
                    )

  
else:
  gl_accounts_ebs = spark.table('s_finance.gl_accounts_ebs').filter("_ID != '0'")
 

# COMMAND ----------

if sampling:
  gl_accounts_ebs = gl_accounts_ebs.limit(10)

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

gl_accounts_ebs_f = (
  gl_accounts_ebs
  .select(columns)
  .transform(apply_schema(schema))
)

# COMMAND ----------

main_f = (
         gl_accounts_ebs_f
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
  
  cutoff_value = get_incr_col_max_value(gl_accounts_ebs, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_finance.gl_accounts_ebs')
  update_run_datetime(run_datetime, table_name, 's_finance.gl_accounts_ebs')
  

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_finance
