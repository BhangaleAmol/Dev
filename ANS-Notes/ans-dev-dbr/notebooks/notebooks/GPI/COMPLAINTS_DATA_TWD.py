# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.1/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.1/header_silver_notebook

# COMMAND ----------

# INPUT PARAMETERS
run_datetime = get_param(params, "run_datetime")
target_folder = get_param(params, "target_folder", "string", "/datalake_silver/core/full_data")
database_name = get_param(params, "database_name", "string", "g_gpi")
table_name = get_param(params, "table_name", "string", "complaints_data_v2")
incremental = get_param(params, "incremental", "bool", False)
overwrite = get_param(params, "overwrite", "bool", False)
print_dict(params)

# COMMAND ----------

# LOAD MAIN DATASET
cutoff_value = get_cutoff_value('s_service.quality_complaint_defects_twd')
main_inc = load_main_dataset('s_service.quality_complaint_defects_twd', 'updatedOn', cutoff_value)
main_inc.createOrReplaceTempView('main_inc')
display(main_inc)

# COMMAND ----------

main = spark.sql("""
SELECT
  DISTINCT c.createdOn,
  c.modifiedOn,
  c.complaintId Record_ID,
  TO_DATE(
    DATE_FORMAT(c.createdOn, 'dd-MM-yyyy'),
    'dd-MM-yyyy'
  ) Date_Created,
  fiscalYear AS Fiscal_year,
  DATE_FORMAT(c.createdOn, 'MMM') Month,
  DATE_FORMAT(c.closedDate, 'dd-MM-yyyy') Date_Closed,
  c.address1country Country,
  c.region Region,
  p.productCode AS Product_ID,
  COALESCE(p.itemId, 'No Item ID') AS Item_ID,
  c.sbu SBU,
  p.productBrand AS Brand,
  p.productStyle AS Style,
  p.productSubBrand Sub_Brand,
  d.complaintType Defect,
  c.customerName Customer_Name,
  c.gbu GBU,
  totalResponseTime Total_Response_Time,
  1 Count
FROM
  main_inc d
  INNER JOIN s_service.quality_complaint_twd c ON d.complaint_id = c._ID
  LEFT OUTER JOIN s_core.product_agg p ON c.complaintProduct_ID = p._ID
WHERE
  YEAR(ADD_MONTHS(c.createdOn, 6)) > YEAR(ADD_MONTHS(CURRENT_DATE, 6)) - 2
""")

main.cache()
display(main)

# COMMAND ----------

# MAGIC %run ../_SCHEMA/g_gpi.complaints_data

# COMMAND ----------

# ATTACH_INTERNAL_DATA
main_f = (
  main    
    .transform(attach_surrogate_key(columns = 'Record_id,Item_ID,Defect'))
    .transform(attach_partition_column("createdOn"))
    .transform(apply_schema(schema))
)
display(main_f)

# COMMAND ----------

# VALIDATE DATA
valid_count_rows(main_f, '_ID')

# COMMAND ----------

# CHECK DATA QUALITY
show_null_values(main_f)

# COMMAND ----------

# MERGE DATA
register_hive_table(main_f, database_name, table_name, target_folder, partition_column = ["_PART"],overwrite=overwrite)
merge_into_hive_table(main_f, key_columns = '_ID', auto_merge = False)

# COMMAND ----------

# UPDATE CUTOFF VALUE
cutoff_value = get_incr_col_max_value(main_inc, 'updatedOn')
update_cutoff_value(cutoff_value, 's_service.quality_complaint_defects_twd')
update_run_datetime(run_datetime, 's_service.quality_complaint_defects_twd')
