# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_supplychain

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value =   get_cutoff_value(table_name, 's_supplychain.sales_invoice_headers_col', prune_days)
  sales_invoice_headers_col = ( 
                                spark.table('s_supplychain.sales_invoice_headers_col')
                                .filter(f"_MODIFIED > '{cutoff_value}'")
                              ) 
  
  cutoff_value = get_cutoff_value(table_name, 's_supplychain.sales_invoice_headers_ebs', prune_days)
  sales_invoice_headers_ebs = ( 
                                spark.table('s_supplychain.sales_invoice_headers_ebs')
                                .filter(f"_MODIFIED > '{cutoff_value}'")
                              ) 
  
  cutoff_value = get_cutoff_value(table_name, 's_supplychain.sales_invoice_headers_kgd', prune_days)
  sales_invoice_headers_kgd = ( 
                                spark.table('s_supplychain.sales_invoice_headers_kgd')
                                .filter(f"_MODIFIED > '{cutoff_value}'")
                              ) 
  
  cutoff_value = get_cutoff_value(table_name, 's_supplychain.sales_invoice_headers_sap', prune_days)
  sales_invoice_headers_sap = ( 
                                spark.table('s_supplychain.sales_invoice_headers_sap')
                                .filter(f"_MODIFIED > '{cutoff_value}'")
                              ) 
  
  cutoff_value =   get_cutoff_value(table_name, 's_supplychain.sales_invoice_headers_tot', prune_days)
  sales_invoice_headers_tot = ( 
                                spark.table('s_supplychain.sales_invoice_headers_tot')
                                .filter(f"_MODIFIED > '{cutoff_value}'")
                              ) 
else:
  sales_invoice_headers_col   = spark.table('s_supplychain.sales_invoice_headers_col')
  sales_invoice_headers_ebs   = spark.table('s_supplychain.sales_invoice_headers_ebs')
  sales_invoice_headers_kgd   = spark.table('s_supplychain.sales_invoice_headers_kgd')
  sales_invoice_headers_sap   = spark.table('s_supplychain.sales_invoice_headers_sap')
  sales_invoice_headers_tot   = spark.table('s_supplychain.sales_invoice_headers_tot')

# COMMAND ----------

sales_invoice_headers_col  = sales_invoice_headers_col.filter("_ID != '0'")
sales_invoice_headers_ebs  = sales_invoice_headers_ebs.filter("_ID != '0'")
sales_invoice_headers_kgd  = sales_invoice_headers_kgd.filter("_ID != '0'")
sales_invoice_headers_sap  = sales_invoice_headers_sap.filter("_ID != '0'")
sales_invoice_headers_tot  = sales_invoice_headers_tot.filter("_ID != '0'")  

# COMMAND ----------

# SAMPLING
if sampling:
  sales_invoice_headers_col  = sales_invoice_headers_col.limit(10)
  sales_invoice_headers_ebs  = sales_invoice_headers_ebs.limit(10)
  sales_invoice_headers_kgd  = sales_invoice_headers_kgd.limit(10) 
  sales_invoice_headers_sap  = sales_invoice_headers_sap.limit(10)
  sales_invoice_headers_tot  = sales_invoice_headers_tot.limit(10)

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_supplychain.sales_invoice_headers

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

sales_invoice_headers_col_f = (
  sales_invoice_headers_col
  .select(columns)
  .transform(apply_schema(schema))
)

sales_invoice_headers_ebs_f = (
  sales_invoice_headers_ebs
  .select(columns)
  .transform(apply_schema(schema))
)

sales_invoice_headers_kgd_f = (
  sales_invoice_headers_kgd
  .select(columns)
  .transform(apply_schema(schema))
)

sales_invoice_headers_sap_f = (
  sales_invoice_headers_sap
  .select(columns)
  .transform(apply_schema(schema))
)

sales_invoice_headers_tot_f = (
  sales_invoice_headers_tot
  .select(columns)
  .transform(apply_schema(schema))
)

# COMMAND ----------

main_f = (
         sales_invoice_headers_col_f  
  .union(sales_invoice_headers_ebs_f  )
  .union(sales_invoice_headers_kgd_f  )
  .union(sales_invoice_headers_sap_f  )
  .union(sales_invoice_headers_tot_f  )
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
update_foreign_key(table_name, 'customerId,_SOURCE', 'customer_ID', 'edm.account')
update_foreign_key(table_name, 'customerId, customerDivision, salesOrganization, distributionChannel,_SOURCE', 'customerOrganization_ID', 'edm.account_organization')

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not (test_run or sampling):
  cutoff_value = get_incr_col_max_value(sales_invoice_headers_col, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_supplychain.sales_invoice_headers_col')
  update_run_datetime(run_datetime, table_name, 's_supplychain.sales_invoice_headers_col')
  
  cutoff_value = get_incr_col_max_value(sales_invoice_headers_ebs, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_supplychain.sales_invoice_headers_ebs')
  update_run_datetime(run_datetime, table_name, 's_supplychain.sales_invoice_headers_ebs')
  
  cutoff_value = get_incr_col_max_value(sales_invoice_headers_kgd, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_supplychain.sales_invoice_headers_kgd')
  update_run_datetime(run_datetime, table_name, 's_supplychain.sales_invoice_headers_kgd')  
  
  cutoff_value = get_incr_col_max_value(sales_invoice_headers_sap, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_supplychain.sales_invoice_headers_sap')
  update_run_datetime(run_datetime, table_name, 's_supplychain.sales_invoice_headers_sap') 
  
  cutoff_value = get_incr_col_max_value(sales_invoice_headers_tot, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_supplychain.sales_invoice_headers_tot')
  update_run_datetime(run_datetime, table_name, 's_supplychain.sales_invoice_headers_tot')  

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_supplychain
