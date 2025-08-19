# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_trademanagement

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_tm.pointofsales

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 's_trademanagement.point_of_sales_ebs', prune_days)  
  point_of_sales_ebs = (
    spark.table('s_trademanagement.point_of_sales_ebs')
    .filter(f"_MODIFIED > '{cutoff_value}'")
    .filter("_ID != '0'")
  )  
else:
  point_of_sales_ebs = spark.table('s_trademanagement.point_of_sales_ebs').filter("_ID != '0'")
  
point_of_sales_adj = spark.table('s_trademanagement.point_of_sales_adj').filter("_ID != '0'")  
point_of_sales_grainger = spark.table('s_trademanagement.point_of_sales_grainger').filter("_ID != '0'")
point_of_sales_hs = spark.table('s_trademanagement.point_of_sales_hs').filter("_ID != '0'")
point_of_sales_hscanada = spark.table('s_trademanagement.point_of_sales_hscanada').filter("_ID != '0'")
point_of_sales_ndc = spark.table('s_trademanagement.point_of_sales_ndc').filter("_ID != '0'")
point_of_sales_airgas = spark.table('s_trademanagement.point_of_sales_airgas').filter("_ID != '0'")

# COMMAND ----------

# SAMPLING
if sampling:
  point_of_sales_adj = point_of_sales_adj.limit(10)
  point_of_sales_ebs = point_of_sales_ebs.limit(10)
  point_of_sales_grainger = point_of_sales_grainger.limit(10) 
  point_of_sales_hs = point_of_sales_hs.limit(10)
  point_of_sales_hscanada = point_of_sales_hscanada.limit(10)
  point_of_sales_ndc = point_of_sales_ndc.limit(10)
  point_of_sales_airgas = point_of_sales_airgas.limit(10)

# COMMAND ----------

columns = list(schema.keys())

point_of_sales_adj_f = (
  point_of_sales_adj
  .select(columns)
  .transform(apply_schema(schema))
)

point_of_sales_adj_f = (
  point_of_sales_adj
  .select(columns)
  .transform(apply_schema(schema))
)

point_of_sales_ebs_f = (
  point_of_sales_ebs
  .select(columns)
  .transform(apply_schema(schema))
)

point_of_sales_grainger_f = (
  point_of_sales_grainger
  .select(columns)
  .transform(apply_schema(schema))
)

point_of_sales_hs_f = (
  point_of_sales_hs
  .select(columns)
  .transform(apply_schema(schema))
)

point_of_sales_hscanada_f = (
  point_of_sales_hscanada
  .select(columns)
  .transform(apply_schema(schema))
)

point_of_sales_ndc_f = (
  point_of_sales_ndc
  .select(columns)
  .transform(apply_schema(schema))
)

point_of_sales_airgas_f = (
  point_of_sales_airgas
  .select(columns)
  .transform(apply_schema(schema))
)

# COMMAND ----------

main_f = (
  point_of_sales_adj_f
  .union(point_of_sales_ebs_f)
  .union(point_of_sales_grainger_f)
  .union(point_of_sales_hs_f)
  .union(point_of_sales_hscanada_f)
  .union(point_of_sales_ndc_f)
  .union(point_of_sales_airgas_f)
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
  cutoff_value = get_incr_col_max_value(point_of_sales_ebs, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_trademanagement.point_of_sales_ebs')
  update_run_datetime(run_datetime, table_name, 's_trademanagement.point_of_sales_ebs')  

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_trademanagement
