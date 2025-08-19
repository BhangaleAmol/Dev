# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# LOAD DATASETS
if incremental:
  
  cutoff_value = get_cutoff_value(table_name, 's_core.transaction_type_col', prune_days) 
  transaction_type_col =  (
                            spark.table('s_core.transaction_type_col')
                            .filter(f"_MODIFIED > '{cutoff_value}'")
                          )
                          
  cutoff_value = get_cutoff_value(table_name, 's_core.transaction_type_ebs', prune_days) 
  transaction_type_ebs = (
                            spark.table('s_core.transaction_type_ebs')
                            .filter(f"_MODIFIED > '{cutoff_value}'")
                          )
  
  cutoff_value = get_cutoff_value(table_name, 's_core.transaction_type_kgd', prune_days) 
  transaction_type_kgd = (
                            spark.table('s_core.transaction_type_kgd')
                            .filter(f"_MODIFIED > '{cutoff_value}'")
                          )
    
  cutoff_value = get_cutoff_value(table_name, 's_core.transaction_type_sap', prune_days) 
  transaction_type_sap = (
                            spark.table('s_core.transaction_type_sap')
                            .filter(f"_MODIFIED > '{cutoff_value}'")
                          )
   
  cutoff_value = get_cutoff_value(table_name, 's_core.transaction_type_tot', prune_days) 
  transaction_type_tot = (
                            spark.table('s_core.transaction_type_tot')
                            .filter(f"_MODIFIED > '{cutoff_value}'")
                          )
  
  cutoff_value = get_cutoff_value(table_name, 's_core.transaction_type_sf', prune_days) 
  transaction_type_sf = (
                            spark.table('s_core.transaction_type_sf')
                            .filter(f"_MODIFIED > '{cutoff_value}'")  
                          )
  
  
else:
  transaction_type_col = spark.table('s_core.transaction_type_col')
  transaction_type_ebs = spark.table('s_core.transaction_type_ebs')
  transaction_type_kgd = spark.table('s_core.transaction_type_kgd')
  transaction_type_sap = spark.table('s_core.transaction_type_sap')
  transaction_type_tot = spark.table('s_core.transaction_type_tot')
  transaction_type_sf = spark.table('s_core.transaction_type_sf')  

# COMMAND ----------

transaction_type_col = transaction_type_col.filter("_ID != '0'")
transaction_type_ebs = transaction_type_ebs.filter("_ID != '0'")
transaction_type_kgd = transaction_type_kgd.filter("_ID != '0'")
transaction_type_sap = transaction_type_sap.filter("_ID != '0'")
transaction_type_tot = transaction_type_tot.filter("_ID != '0'")
transaction_type_sf = transaction_type_sf.filter("_ID != '0'")

# COMMAND ----------

# SAMPLING
if sampling:
  transaction_type_col = transaction_type_col.limit(10)
  transaction_type_ebs = transaction_type_ebs.limit(10)
  transaction_type_kgd = transaction_type_kgd.limit(10)
  transaction_type_sap = transaction_type_sap.limit(10)
  transaction_type_tot = transaction_type_tot.limit(10)
  transaction_type_sf = transaction_type_sf.limit(10)

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.transaction_type

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

transaction_type_col_f = (
  transaction_type_col
  .select(columns)
  .transform(apply_schema(schema))
)


transaction_type_ebs_f = (
  transaction_type_ebs
  .select(columns)
  .transform(apply_schema(schema))
)


transaction_type_kgd_f = (
  transaction_type_kgd
  .select(columns)
  .transform(apply_schema(schema))
)


transaction_type_sap_f = (
  transaction_type_sap
  .select(columns)
  .transform(apply_schema(schema))
)

transaction_type_tot_f = (
  transaction_type_tot
  .select(columns)
  .transform(apply_schema(schema))
)

transaction_type_sf_f = (
  transaction_type_sf
  .select(columns)
  .transform(apply_schema(schema))
)

# COMMAND ----------

main_f = (
  transaction_type_col_f
  .union(transaction_type_ebs_f)
  .union(transaction_type_kgd_f)
  .union(transaction_type_sap_f)
  .union(transaction_type_tot_f)
  .union(transaction_type_sf_f)
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
  cutoff_value = get_incr_col_max_value(transaction_type_col, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.transaction_type_col')
  update_run_datetime(run_datetime, table_name, 's_core.transaction_type_col')
  
  cutoff_value = get_incr_col_max_value(transaction_type_ebs, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.transaction_type_ebs')
  update_run_datetime(run_datetime, table_name, 's_core.transaction_type_ebs')
  
  cutoff_value = get_incr_col_max_value(transaction_type_kgd, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.transaction_type_kgd')
  update_run_datetime(run_datetime, table_name, 's_core.transaction_type_kgd')
  
  cutoff_value = get_incr_col_max_value(transaction_type_sap, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.transaction_type_sap')
  update_run_datetime(run_datetime, table_name, 's_core.transaction_type_sap')
  
  cutoff_value = get_incr_col_max_value(transaction_type_tot, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.transaction_type_tot')
  update_run_datetime(run_datetime, table_name, 's_core.transaction_type_tot')  
  
  cutoff_value = get_incr_col_max_value(transaction_type_sf, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.transaction_type_sf')
  update_run_datetime(run_datetime, table_name, 's_core.transaction_type_sf')  

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core
