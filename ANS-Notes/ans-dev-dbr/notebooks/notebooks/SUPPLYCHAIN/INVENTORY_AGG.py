# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_supplychain

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 's_supplychain.inventory_col', prune_days)
  inventory_col = ( 
                    spark.table('s_supplychain.inventory_col')
                    .filter(f"_MODIFIED > '{cutoff_value}'")
                  )
  
  cutoff_value = get_cutoff_value(table_name, 's_supplychain.inventory_ebs', prune_days)
  inventory_ebs = ( 
                    spark.table('s_supplychain.inventory_ebs')
                    .filter(f"_MODIFIED > '{cutoff_value}'")
                  )
  
  cutoff_value = get_cutoff_value(table_name, 's_supplychain.inventory_kgd', prune_days)
  inventory_kgd = ( 
                    spark.table('s_supplychain.inventory_kgd')
                    .filter(f"_MODIFIED > '{cutoff_value}'")
                  )
  
  cutoff_value = get_cutoff_value(table_name, 's_supplychain.inventory_sap', prune_days)
  inventory_sap = ( 
                    spark.table('s_supplychain.inventory_sap')
                    .filter(f"_MODIFIED > '{cutoff_value}'")
                  )
  
  cutoff_value = get_cutoff_value(table_name, 's_supplychain.inventory_tot', prune_days)
  inventory_tot = ( 
                    spark.table('s_supplychain.inventory_tot')
                    .filter(f"_MODIFIED > '{cutoff_value}'")
                  )
  
else:
  inventory_col   = spark.table('s_supplychain.inventory_col')
  inventory_ebs   = spark.table('s_supplychain.inventory_ebs')
  inventory_kgd   = spark.table('s_supplychain.inventory_kgd')
  inventory_sap   = spark.table('s_supplychain.inventory_sap')
  inventory_tot   = spark.table('s_supplychain.inventory_tot')

# COMMAND ----------

inventory_col  = inventory_col.filter("_ID != '0'")
inventory_ebs  = inventory_ebs.filter("_ID != '0'")
inventory_kgd  = inventory_kgd.filter("_ID != '0'")
inventory_sap  = inventory_sap.filter("_ID != '0'")
inventory_tot  = inventory_tot.filter("_ID != '0'")

# COMMAND ----------

# SAMPLING
if sampling:
  inventory_col  = inventory_col.limit(10)
  inventory_ebs  = inventory_ebs.limit(10)
  inventory_kgd  = inventory_kgd.limit(10) 
  inventory_sap  = inventory_sap.limit(10)
  inventory_tot  = inventory_tot.limit(10)

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_supplychain.inventory

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

inventory_col_f = (
  inventory_col
  .select(columns)
  .transform(apply_schema(schema))
)

inventory_ebs_f = (
  inventory_ebs
  .select(columns)
  .transform(apply_schema(schema))
)

inventory_kgd_f = (
  inventory_kgd
  .select(columns)
  .transform(apply_schema(schema))
)

inventory_sap_f = (
  inventory_sap
  .select(columns)
  .transform(apply_schema(schema))
)

inventory_tot_f = (
  inventory_tot
  .select(columns)
  .transform(apply_schema(schema))
)

# COMMAND ----------

main_f = (
         inventory_col_f  
  .union(inventory_ebs_f  )
  .union(inventory_kgd_f  )
  .union(inventory_sap_f  )
  .union(inventory_tot_f  )
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
  cutoff_value = get_incr_col_max_value(inventory_col, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_supplychain.inventory_col')
  update_run_datetime(run_datetime, table_name, 's_supplychain.inventory_col')
  
  cutoff_value = get_incr_col_max_value(inventory_ebs, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_supplychain.inventory_ebs')
  update_run_datetime(run_datetime, table_name, 's_supplychain.inventory_ebs')
  
  cutoff_value = get_incr_col_max_value(inventory_kgd, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_supplychain.inventory_kgd')
  update_run_datetime(run_datetime, table_name, 's_supplychain.inventory_kgd') 
  
  cutoff_value = get_incr_col_max_value(inventory_sap, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_supplychain.inventory_sap')
  update_run_datetime(run_datetime, table_name, 's_supplychain.invnetory_sap')
  
  cutoff_value = get_incr_col_max_value(inventory_tot, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_supplychain.inventory_tot')
  update_run_datetime(run_datetime, table_name, 's_supplychain.inventory_tot')  

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_supplychain
