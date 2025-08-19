# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# LOAD DATASETS
if incremental:
  
  cutoff_value = get_cutoff_value(table_name, 's_core.organization_col', prune_days) 
  organization_col = ( 
                        spark.table('s_core.organization_col')
                       .filter(f"_MODIFIED > '{cutoff_value}'")
                     )
  
  cutoff_value = get_cutoff_value(table_name, 's_core.organization_ebs', prune_days) 
  organization_ebs = ( 
                        spark.table('s_core.organization_ebs')
                       .filter(f"_MODIFIED > '{cutoff_value}'")
                     )
  
  cutoff_value = get_cutoff_value(table_name, 's_core.organization_pdh', prune_days) 
  organization_pdh = ( 
                        spark.table('s_core.organization_pdh')
                       .filter(f"_MODIFIED > '{cutoff_value}'")
                     )
   
  cutoff_value = get_cutoff_value(table_name, 's_core.organization_sap', prune_days) 
  organization_sap = ( 
                        spark.table('s_core.organization_sap')
                       .filter(f"_MODIFIED > '{cutoff_value}'")
                     )
  
  cutoff_value = get_cutoff_value(table_name, 's_core.organization_tot', prune_days) 
  organization_tot = ( 
                        spark.table('s_core.organization_tot')
                       .filter(f"_MODIFIED > '{cutoff_value}'")
                     )
 
  cutoff_value = get_cutoff_value(table_name, 's_core.organization_kgd', prune_days) 
  organization_kgd = ( 
                        spark.table('s_core.organization_kgd')
                       .filter(f"_MODIFIED > '{cutoff_value}'")
                     )
  
else:
  organization_col = spark.table('s_core.organization_col')
  organization_ebs = spark.table('s_core.organization_ebs')
  organization_pdh = spark.table('s_core.organization_pdh')
  organization_sap = spark.table('s_core.organization_sap')
  organization_tot = spark.table('s_core.organization_tot')
  organization_kgd = spark.table('s_core.organization_kgd')

# COMMAND ----------

organization_col  = organization_col.filter("_ID != '0'")
organization_ebs  = organization_ebs.filter("_ID != '0'")
organization_pdh  = organization_pdh.filter("_ID != '0'")
organization_sap  = organization_sap.filter("_ID != '0'")
organization_tot  = organization_tot.filter("_ID != '0'")
organization_kgd  = organization_kgd.filter("_ID != '0'")

# COMMAND ----------

# SAMPLING
if sampling:
  organization_col  = organization_col.limit(10)
  organization_ebs  = organization_ebs.limit(10)
  organization_pdh  = organization_pdh.limit(10) 
  organization_sap  = organization_sap.limit(10)
  organization_tot  = organization_tot.limit(10)
  organization_kgd  = organization_kgd.limit(10)

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.organization

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

organization_col_f = (
  organization_col
  .select(columns)
  .transform(apply_schema(schema))
)

organization_ebs_f = (
  organization_ebs
  .select(columns)
  .transform(apply_schema(schema))
)

organization_pdh_f = (
  organization_pdh
  .select(columns)
  .transform(apply_schema(schema))
)

organization_sap_f = (
  organization_sap
  .select(columns)
  .transform(apply_schema(schema))
)

organization_tot_f = (
  organization_tot
  .select(columns)
  .transform(apply_schema(schema))
)

organization_kgd_f = (
  organization_kgd
  .select(columns)
  .transform(apply_schema(schema))
)

# COMMAND ----------

main_f = (
  organization_col
  .union(organization_ebs)
  .union(organization_pdh)
  .union(organization_sap)
  .union(organization_tot)
  .union(organization_kgd)
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
  cutoff_value = get_incr_col_max_value(organization_col, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.organization_col')
  update_run_datetime(run_datetime, table_name, 's_core.organization_col')
  
  cutoff_value = get_incr_col_max_value(organization_ebs, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.organization_ebs')
  update_run_datetime(run_datetime, table_name, 's_core.organization_ebs')
  
  cutoff_value = get_incr_col_max_value(organization_pdh, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.organization_pdh')
  update_run_datetime(run_datetime, table_name, 's_core.organization_pdh')
  
  cutoff_value = get_incr_col_max_value(organization_sap, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.organization_sap')
  update_run_datetime(run_datetime, table_name, 's_core.organization_sap')
  
  cutoff_value = get_incr_col_max_value(organization_tot, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.organization_tot')
  update_run_datetime(run_datetime, table_name, 's_core.organization_tot')
  
  cutoff_value = get_incr_col_max_value(organization_kgd, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.organization_kgd')
  update_run_datetime(run_datetime, table_name, 's_core.organization_kgd')  

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core
