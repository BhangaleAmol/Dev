# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_supplychain

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 's_supplychain.sales_invoice_lines_col', prune_days)
  sales_invoice_lines_col = ( 
                                spark.table('s_supplychain.sales_invoice_lines_col')
                                .filter(f"_MODIFIED > '{cutoff_value}'")
                              ) 
  
  cutoff_value = get_cutoff_value(table_name, 's_supplychain.sales_invoice_lines_ebs', prune_days)
  sales_invoice_lines_ebs = ( 
                                spark.table('s_supplychain.sales_invoice_lines_ebs')
                                .filter(f"_MODIFIED > '{cutoff_value}'")
                              ) 
  
  cutoff_value = get_cutoff_value(table_name, 's_supplychain.sales_invoice_lines_kgd', prune_days)
  sales_invoice_lines_kgd = ( 
                                spark.table('s_supplychain.sales_invoice_lines_kgd')
                                .filter(f"_MODIFIED > '{cutoff_value}'")
                              ) 
  
  cutoff_value = get_cutoff_value(table_name, 's_supplychain.sales_invoice_lines_sap', prune_days)
  sales_invoice_lines_sap = ( 
                                spark.table('s_supplychain.sales_invoice_lines_sap')
                                .filter(f"_MODIFIED > '{cutoff_value}'")
                              ) 
  
  cutoff_value = get_cutoff_value(table_name, 's_supplychain.sales_invoice_lines_tot', prune_days)
  sales_invoice_lines_tot = ( 
                                spark.table('s_supplychain.sales_invoice_lines_tot')
                                .filter(f"_MODIFIED > '{cutoff_value}'")
                              ) 
else:
  sales_invoice_lines_col   = spark.table('s_supplychain.sales_invoice_lines_col')
  sales_invoice_lines_ebs   = spark.table('s_supplychain.sales_invoice_lines_ebs')
  sales_invoice_lines_kgd   = spark.table('s_supplychain.sales_invoice_lines_kgd')
  sales_invoice_lines_sap   = spark.table('s_supplychain.sales_invoice_lines_sap')
  sales_invoice_lines_tot   = spark.table('s_supplychain.sales_invoice_lines_tot')

# COMMAND ----------

sales_invoice_lines_col  = sales_invoice_lines_col.filter("_ID != '0'")
sales_invoice_lines_ebs  = sales_invoice_lines_ebs.filter("_ID != '0'")
sales_invoice_lines_kgd  = sales_invoice_lines_kgd.filter("_ID != '0'")
sales_invoice_lines_sap  = sales_invoice_lines_sap.filter("_ID != '0'")
sales_invoice_lines_tot  = sales_invoice_lines_tot.filter("_ID != '0'")

# COMMAND ----------

# SAMPLING
if sampling:
  sales_invoice_lines_col  = sales_invoice_lines_col.limit(10)
  sales_invoice_lines_ebs  = sales_invoice_lines_ebs.limit(10)
  sales_invoice_lines_kgd  = sales_invoice_lines_kgd.limit(10)
  sales_invoice_lines_sap  = sales_invoice_lines_sap.limit(10)
  sales_invoice_lines_tot  = sales_invoice_lines_tot.limit(10)

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_supplychain.sales_invoice_lines

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

sales_invoice_lines_col_f = (
  sales_invoice_lines_col
  .select(columns)
  .transform(apply_schema(schema))
)

sales_invoice_lines_ebs_f = (
  sales_invoice_lines_ebs
  .select(columns)
  .transform(apply_schema(schema))
)

sales_invoice_lines_kgd_f = (
  sales_invoice_lines_kgd
  .select(columns)
  .transform(apply_schema(schema))
)

sales_invoice_lines_sap_f = (
  sales_invoice_lines_sap
  .select(columns)
  .transform(apply_schema(schema))
)

sales_invoice_lines_tot_f = (
  sales_invoice_lines_tot
  .select(columns)
  .transform(apply_schema(schema))
)

# COMMAND ----------

main_f = (
         sales_invoice_lines_col_f 
  .union(sales_invoice_lines_ebs_f )
  .union(sales_invoice_lines_kgd_f )
  .union(sales_invoice_lines_sap_f )
  .union(sales_invoice_lines_tot_f )
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
  cutoff_value = get_incr_col_max_value(sales_invoice_lines_col, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_supplychain.sales_invoice_lines_col')
  update_run_datetime(run_datetime, table_name, 's_supplychain.sales_invoice_lines_col')
  
  cutoff_value = get_incr_col_max_value(sales_invoice_lines_ebs, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_supplychain.sales_invoice_lines_ebs')
  update_run_datetime(run_datetime, table_name, 's_supplychain.sales_invoice_lines_ebs')
  
  cutoff_value = get_incr_col_max_value(sales_invoice_lines_kgd, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_supplychain.sales_invoice_lines_kgd')
  update_run_datetime(run_datetime, table_name, 's_supplychain.sales_invoice_lines_kgd')  
  
  cutoff_value = get_incr_col_max_value(sales_invoice_lines_sap, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_supplychain.sales_invoice_lines_sap')
  update_run_datetime(run_datetime, table_name, 's_supplychain.sales_invoice_lines_sap') 
  
  cutoff_value = get_incr_col_max_value(sales_invoice_lines_tot, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_supplychain.sales_invoice_lines_tot')
  update_run_datetime(run_datetime, table_name, 's_supplychain.sales_invoice_lines_tot')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_supplychain
