# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_silver_notebook

# COMMAND ----------

# DEFAULT PARAMETERS
database_name = database_name or "s_manufacturing"
table_name = table_name or "production_data_xia"
target_folder = target_folder or "/datalake_silver/manufacturing/full_data"
source_name = "AXL"

# COMMAND ----------

# SETUP PARAMETERS
table_name = get_table_name(database_name, None, table_name)
table_name_agg = table_name[:table_name.rfind("_")] + '_agg'

# COMMAND ----------

# INPUT PARAMETERS
print_silver_params()

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'gpd.xia_production_data')
  main_inc = load_incr_dataset('gpd.xia_production_data', 'Modified', cutoff_value)
else:
  main_inc = load_full_dataset('gpd.xia_production_data')  

# COMMAND ----------

# SAMPLING
if sampling:
  main_inc = main_inc.limit(10)

# COMMAND ----------

# VIEWS
main_inc.createOrReplaceTempView('main_inc')

# COMMAND ----------

XIA_PRODUCTION_DATA_F = spark.sql("""
  SELECT
    Id AS ID,
    ProductId AS PRODUCT_ID,
    DATE_FORMAT(Date, "yyyy-MM-dd") AS DATE,
    'AXL' AS PLANT,    
    ProductionLineValue AS MACHINE,
    Process AS PROCESS,
    ActualProduction AS ACTUAL_PRODUCTION,
    PlannedDailyOutput AS TARGET_PRODUCTION,
    NULL AS WORKING_TIME_MIN,
    PlannedWorkingHours AS TARGET_WORKING_TIME_MIN,
    TotalRejects AS TOTAL_REJECTS,
    NULL AS TOTAL_DOWNTIME_MIN,
    SewingRejects AS TOTAL_REWORKS,
    DATE_FORMAT(Created, "yyyy-MM-dd hh:mm:ss") AS CREATED,
    DATE_FORMAT(Modified, "yyyy-MM-dd hh:mm:ss") AS MODIFIED,
    Headcount AS HEADCOUNT,
    PPHTarget AS PPH_TARGET,
    AttendanceHours AS ATTENDANCE_HOURS,
    FabricTotalInput AS FABRIC_TOTAL_INPUT,
    FAbricTotalOutput AS FABRIC_TOTAL_OUTPUT
  FROM main_inc
""")
XIA_PRODUCTION_DATA_F.createOrReplaceTempView("XIA_PRODUCTION_DATA_F")
display(XIA_PRODUCTION_DATA_F)

# COMMAND ----------

PRODUCTS_LIST_F = spark.sql("""
  SELECT
    Id AS ID,
    Product AS PRODUCT,
    ProductGroup AS PRODUCT_GROUP,
    GBU,    
    SBU
  FROM gpd.xia_products_list
""")
PRODUCTS_LIST_F.createOrReplaceTempView("PRODUCTS_LIST_F")
display(PRODUCTS_LIST_F)

# COMMAND ----------

valid_count_rows(PRODUCTS_LIST_F, "ID,PRODUCT", 'PRODUCTS_LIST_F')

# COMMAND ----------

main = spark.sql("""
  SELECT
    pd.ID,
    pd.DATE,
    pd.PLANT,
    pl.GBU,
    pl.SBU,
    pd.MACHINE,
    pd.PROCESS,
    pl.PRODUCT_GROUP,
    pl.PRODUCT,
    pd.ACTUAL_PRODUCTION,
    pd.TARGET_PRODUCTION,
    pd.WORKING_TIME_MIN,
    pd.TARGET_WORKING_TIME_MIN,
    pd.TOTAL_DOWNTIME_MIN,
    pd.TOTAL_REJECTS,
    pd.TOTAL_REWORKS,
    pd.CREATED,
    pd.MODIFIED,
    pd.HEADCOUNT,
    pd.PPH_TARGET,
    pd.ATTENDANCE_HOURS,
    pd.FABRIC_TOTAL_INPUT,
    pd.FABRIC_TOTAL_OUTPUT
  FROM XIA_PRODUCTION_DATA_F pd
  LEFT JOIN PRODUCTS_LIST_F pl ON pd.PRODUCT_ID = pl.ID
""")
display(main)

# COMMAND ----------

# NOTIFICATION - RECORDS NOT UPDATED

if(ENV_NAME == 'prod'):
  
  LAST_RECORD = spark.sql("""
    SELECT
      DATEDIFF(CURRENT_DATE(), MAX(DATE)) AS LAST_RECORD_DAYS,
      MAX(DATE) AS LAST_RECORD_DATE
    FROM XIA_PRODUCTION_DATA_F
  """).collect()[0]

  if LAST_RECORD['LAST_RECORD_DAYS'] > 7:
    data = {
      "site": "AXL",
      "list_name": 'XIAMEN_PRODUCTION_DATA',
      "max_date": LAST_RECORD['LAST_RECORD_DATE']    
    }
    mail_gpd_records_not_updated(**data)

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_manufacturing.production_data

# COMMAND ----------

# ATTACH INTERNAL COLUMNS
main_f = (
  main
  .transform(attach_deleted_flag(False))
  .transform(attach_modified_date())
  .transform(attach_source_column(source = source_name, column = "_SOURCE"))
  .transform(attach_surrogate_key(columns = 'ID,_SOURCE'))
  .transform(attach_partition_column("CREATED", 'yyyy-MM-dd hh:mm:ss', 'yyyy-MM'))  
  .transform(apply_schema(schema))
)
main_f.cache()
display(main_f)

# COMMAND ----------

# PERSIST DATA
merge_to_delta(main_f, table_name, target_folder, overwrite)

options = {'auto_merge': True, 'overwrite': overwrite}
merge_to_delta_agg(main_f, table_name_agg, source_name, target_folder, options)

# COMMAND ----------

# HANDLE DELETE
full_keys_f = (
  spark.table('gpd.xia_production_data')
  .filter("_DELETED IS FALSE")
  .select("ID")
  .transform(attach_source_column(source = source_name))
  .transform(attach_surrogate_key(columns = 'ID,_SOURCE'))
  .select('_ID')
)

apply_soft_delete(full_keys_f, table_name, key_columns = '_ID')
apply_soft_delete(full_keys_f, table_name_agg, key_columns = '_ID', source_name = source_name)

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not test_run:
  cutoff_value = get_incr_col_max_value(main_inc, "Modified")
  update_cutoff_value(cutoff_value, table_name, 'gpd.xia_production_data')
  update_run_datetime(run_datetime, table_name, 'gpd.xia_production_data')

# COMMAND ----------


