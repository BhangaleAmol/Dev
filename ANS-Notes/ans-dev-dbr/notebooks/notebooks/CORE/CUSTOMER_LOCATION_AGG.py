# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# LOAD DATASETS
if incremental:
  
  cutoff_value = get_cutoff_value(table_name, 's_core.customer_location_col', prune_days) 
  customer_location_col = ( 
                                spark.table('s_core.customer_location_col')
                                .filter(f"_MODIFIED > '{cutoff_value}'")
                              )
  
  
  cutoff_value = get_cutoff_value(table_name, 's_core.customer_location_ebs', prune_days) 
  customer_location_ebs = ( 
                                spark.table('s_core.customer_location_ebs')
                                .filter(f"_MODIFIED > '{cutoff_value}'")
                              )
  
  cutoff_value = get_cutoff_value(table_name, 's_core.customer_location_kgd', prune_days) 
  customer_location_kgd = ( 
                                spark.table('s_core.customer_location_kgd')
                                .filter(f"_MODIFIED > '{cutoff_value}'")
                              )
    
  cutoff_value = get_cutoff_value(table_name, 's_core.customer_location_sap', prune_days) 
  customer_location_sap = ( 
                                spark.table('s_core.customer_location_sap')
                                .filter(f"_MODIFIED > '{cutoff_value}'")
                              )
  
  cutoff_value = get_cutoff_value(table_name, 's_core.customer_location_tot', prune_days) 
  customer_location_tot = ( 
                                spark.table('s_core.customer_location_tot')
                                .filter(f"_MODIFIED > '{cutoff_value}'")
                              )
    
else:
  customer_location_col = spark.table('s_core.customer_location_col')
  customer_location_ebs = spark.table('s_core.customer_location_ebs')
  customer_location_kgd = spark.table('s_core.customer_location_kgd')
  customer_location_sap = spark.table('s_core.customer_location_sap')
  customer_location_tot = spark.table('s_core.customer_location_tot')

# COMMAND ----------

customer_location_col = customer_location_col.filter("_ID != '0'")
customer_location_ebs = customer_location_ebs.filter("_ID != '0'")
customer_location_kgd = customer_location_kgd.filter("_ID != '0'")
customer_location_sap = customer_location_sap.filter("_ID != '0'")
customer_location_tot = customer_location_tot.filter("_ID != '0'")

# COMMAND ----------

# SAMPLING
if sampling:
  customer_location_col = customer_location_col.limit(10)
  customer_location_ebs = customer_location_ebs.limit(10)
  customer_location_kgd = customer_location_kgd.limit(10) 
  customer_location_sap = customer_location_sap.limit(10)
  customer_location_tot = customer_location_tot.limit(10)

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.customer_location

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

customer_location_col_f = (
  customer_location_col
  .select(columns)
  .transform(apply_schema(schema))
)

customer_location_ebs_f = (
  customer_location_ebs
  .select(columns)
  .transform(apply_schema(schema))
)

customer_location_kgd_f = (
  customer_location_kgd
  .select(columns)
  .transform(apply_schema(schema))
)

customer_location_sap_f = (
  customer_location_sap
  .select(columns)
  .transform(apply_schema(schema))
)

customer_location_tot_f = (
  customer_location_tot
  .select(columns)
  .transform(apply_schema(schema))
)


# COMMAND ----------

main_f = (
  customer_location_col_f
  .union(customer_location_ebs_f)
  .union(customer_location_kgd_f)
  .union(customer_location_sap_f)
  .union(customer_location_tot_f)
  .transform(attach_unknown_record)
  .select(columns)
  .transform(sort_columns)
)

# COMMAND ----------

# PERSIST DATA
options = {'target_storage': target_storage, 'target_container': target_container}
merge_to_delta(main_f, table_name, target_folder, overwrite, options = options)

# COMMAND ----------

update_foreign_key(table_name, 'createdBy,_SOURCE', 'createdBy_ID', 'edm.user')
update_foreign_key(table_name, 'modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user')
update_foreign_key(table_name, 'accountId,_SOURCE', 'account_ID', 'edm.account')

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not (test_run or sampling):
  cutoff_value = get_incr_col_max_value(customer_location_col, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.customer_location_col')
  update_run_datetime(run_datetime, table_name, 's_core.customer_location_col')
  
  cutoff_value = get_incr_col_max_value(customer_location_ebs, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.customer_location_ebs')
  update_run_datetime(run_datetime, table_name, 's_core.customer_location_ebs')
  
  cutoff_value = get_incr_col_max_value(customer_location_kgd, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.customer_location_kgd')
  update_run_datetime(run_datetime, table_name, 's_core.customer_location_kgd')
  
  cutoff_value = get_incr_col_max_value(customer_location_sap, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.customer_location_sap')
  update_run_datetime(run_datetime, table_name, 's_core.customer_location_sap')
  
  cutoff_value = get_incr_col_max_value(customer_location_tot, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.customer_location_tot')
  update_run_datetime(run_datetime, table_name, 's_core.customer_location_tot')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core
