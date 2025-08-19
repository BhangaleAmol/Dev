# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# LOAD DATASETS
if incremental:
  
  cutoff_value = get_cutoff_value(table_name, 's_core.exchange_rate_bud', prune_days) 
  exchange_rate_bud = ( 
                                spark.table('s_core.exchange_rate_bud')
                                .filter(f"_MODIFIED > '{cutoff_value}'")
                              )
  
  cutoff_value = get_cutoff_value(table_name, 's_core.exchange_rate_ebs', prune_days) 
  exchange_rate_ebs = ( 
                                spark.table('s_core.exchange_rate_ebs')
                                .filter(f"_MODIFIED > '{cutoff_value}'")
                              )
  
  cutoff_value = get_cutoff_value(table_name, 's_core.exchange_rate_qv', prune_days) 
  exchange_rate_qv = ( 
                                spark.table('s_core.exchange_rate_qv')
                                .filter(f"_MODIFIED > '{cutoff_value}'")
                              )
  
  
else:
  exchange_rate_bud = spark.table('s_core.exchange_rate_bud')
  exchange_rate_ebs = spark.table('s_core.exchange_rate_ebs')
  exchange_rate_qv = spark.table('s_core.exchange_rate_qv')
  

# COMMAND ----------

exchange_rate_bud = exchange_rate_bud.filter("_ID != '0'")
exchange_rate_ebs = exchange_rate_ebs.filter("_ID != '0'")
exchange_rate_qv = exchange_rate_qv.filter("_ID != '0'")

# COMMAND ----------

# SAMPLING
if sampling:
  exchange_rate_bud = exchange_rate_bud.limit(10)
  exchange_rate_ebs = exchange_rate_ebs.limit(10)
  exchange_rate_qv = exchange_rate_qv.limit(10) 


# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.exchangerate

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

exchange_rate_bud_f = (
  exchange_rate_bud
  .select(columns)
  .transform(apply_schema(schema))
)

exchange_rate_ebs_f = (
  exchange_rate_ebs
  .select(columns)
  .transform(apply_schema(schema))
)

exchange_rate_qv_f = (
  exchange_rate_qv
  .select(columns)
  .transform(apply_schema(schema))
)

# COMMAND ----------

main_f = (
  exchange_rate_bud_f
  .union(exchange_rate_ebs_f)
  .union(exchange_rate_qv_f)
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
  cutoff_value = get_incr_col_max_value(exchange_rate_bud, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.exchange_rate_bud')
  update_run_datetime(run_datetime, table_name, 's_core.exchange_rate_bud')
  
  cutoff_value = get_incr_col_max_value(exchange_rate_ebs, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.exchange_rate_ebs')
  update_run_datetime(run_datetime, table_name, 's_core.exchange_rate_ebs')
  
  cutoff_value = get_incr_col_max_value(exchange_rate_qv, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.exchange_rate_qv')
  update_run_datetime(run_datetime, table_name, 's_core.exchange_rate_qv')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core
