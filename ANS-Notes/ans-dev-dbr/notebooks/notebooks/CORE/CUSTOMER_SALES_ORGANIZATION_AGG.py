# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# LOAD DATASETS
if incremental:
  
  cutoff_value = get_cutoff_value(table_name, 's_core.customer_sales_organization_col', prune_days) 
  customer_sales_organization_col = ( 
                                spark.table('s_core.customer_sales_organization_col')
                                .filter(f"_MODIFIED > '{cutoff_value}'")
                              )
  
  cutoff_value = get_cutoff_value(table_name, 's_core.customer_sales_organization_ebs', prune_days) 
  customer_sales_organization_ebs = ( 
                                spark.table('s_core.customer_sales_organization_ebs')
                                .filter(f"_MODIFIED > '{cutoff_value}'")
                              )
  
  
  cutoff_value = get_cutoff_value(table_name, 's_core.customer_sales_organization_tot', prune_days) 
  customer_sales_organization_tot = ( 
                                spark.table('s_core.customer_sales_organization_tot')
                                .filter(f"_MODIFIED > '{cutoff_value}'")
                              )
    
  
else:
  customer_sales_organization_col = spark.table('s_core.customer_sales_organization_col')
  customer_sales_organization_ebs = spark.table('s_core.customer_sales_organization_ebs')
  customer_sales_organization_tot = spark.table('s_core.customer_sales_organization_tot')

# COMMAND ----------

customer_sales_organization_col = customer_sales_organization_col.filter("_ID != '0'")
customer_sales_organization_ebs = customer_sales_organization_ebs.filter("_ID != '0'")
customer_sales_organization_tot = customer_sales_organization_tot.filter("_ID != '0'")

# COMMAND ----------

# SAMPLING
if sampling:
  customer_sales_organization_col = customer_sales_organization_col.limit(10)
  customer_sales_organization_ebs = customer_sales_organization_ebs.limit(10)
  customer_sales_organization_tot = customer_sales_organization_tot.limit(10) 

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.customer_sales_organization

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

customer_sales_organization_col_f = (
  customer_sales_organization_col
  .select(columns)
  .transform(apply_schema(schema))
)

customer_sales_organization_ebs_f = (
  customer_sales_organization_ebs
  .select(columns)
  .transform(apply_schema(schema))
)

customer_sales_organization_tot_f = (
  customer_sales_organization_tot
  .select(columns)
  .transform(apply_schema(schema))
)

# COMMAND ----------

main_f = (
  customer_sales_organization_col_f
  .union(customer_sales_organization_ebs_f)
  .union(customer_sales_organization_tot_f)
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
update_foreign_key(table_name, 'accountId,_SOURCE', 'account_ID', 'edm.account')

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not (test_run or sampling):
  cutoff_value = get_incr_col_max_value(customer_sales_organization_col, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.customer_sales_organization_col')
  update_run_datetime(run_datetime, table_name, 's_core.customer_sales_organization_col')
  
  cutoff_value = get_incr_col_max_value(customer_sales_organization_ebs, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.customer_sales_organization_ebs')
  update_run_datetime(run_datetime, table_name, 's_core.customer_sales_organization_ebs')
  
  cutoff_value = get_incr_col_max_value(customer_sales_organization_tot, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.customer_sales_organization_tot')
  update_run_datetime(run_datetime, table_name, 's_core.customer_sales_organization_tot')  

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core
