# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run /notebooks/_SHARED/FUNCTIONS/1.2/func_data_validation

# COMMAND ----------

from datetime import datetime

# INPUT PARAMETERS
database_name = get_input_param('database_name', default_value = 'g_logility')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'list', default_value = ['ORD_ID','ORD_LIN_NBR','ITEM_ID','SLVR_DATE','LOC_ID','GenerationDate'])
overwrite = get_input_param('overwrite', 'bool', default_value = False)
prune_days = get_input_param('prune_days', 'int', default_value = 1)
table_name = get_input_param('table_name', default_value = 'logility_sp_dropped_demand')
target_folder = get_input_param('target_folder', default_value = '/datalake_gold/logility/full_data')
temp_folder = get_input_param('temp_folder', default_value = '/datalake_gold/logility/temp_data')
sampling = get_input_param('sampling', 'bool', default_value = False)

# COMMAND ----------

# VALIDATE INPUT
if incremental and key_columns is None:
  raise Exception("INCREMENTAL & NO KEY COLUMNS")

if overwrite and sampling:
  raise Exception("OVERWRITE & SAMPLING")

# COMMAND ----------

# VARIABLES
target_table = get_table_name(database_name,table_name,None)
target_table_agg = table_name[:table_name.rfind("_")] + '_agg'
currentuser = str(spark.sql("select current_user()").collect()[0][0])
currenttime = datetime.now()
source_table = 'logftp.logility_sp_dropped_demands_export'
viewName = 'logility_sp_dropped_demands_export'

# COMMAND ----------

# DBTITLE 1,Determine current runPeriod
# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(source_table, None, prune_days)
  print(cutoff_value)
  logility_sp_dd_extract = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  logility_sp_dd_extract = load_full_dataset(source_table)

logility_sp_dd_extract.createOrReplaceTempView(viewName)
logility_sp_dd_extract.display()

# COMMAND ----------

# DBTITLE 1,Combine Forecast & Demand
main = spark.sql("""
SELECT 
DMD_TYPE,
case when DMD_TYPE=0 then 'Customer Order'
     when DMD_TYPE=1 then 'Forecast'
     when DMD_TYPE=2 then 'Allocation Peg'
     when DMD_TYPE=4 then 'Safety Stock'
     when DMD_TYPE=5 then 'Deficit'
else DMD_TYPE end as Demand_Type_Description,
ORD_ID,
ORD_LIN_NBR,
ORD_QTY,
ITEM_ID,
LOC_ID,
to_date(replace(SLVR_DATE,':000000000',''),'yyyyMMdd') as SLVR_DATE,
CUST_LOC_ID,
VNDR_INDX,
USR_11_TEXT as Drop_Reason,
GenerationDate,
current_timestamp() as _MODIFIED
FROM 
logility_sp_dropped_demands_export
""")
main.display()
main.createOrReplaceTempView("main")

# COMMAND ----------

# VALIDATE DATA
valid_count_rows(main, key_columns)

# COMMAND ----------

register_hive_table(main, target_table, target_folder, options = {'overwrite': overwrite})
append_into_table(main, target_table, {"incremental_column":"GenerationDate"})

# COMMAND ----------

# UPDATE CUTOFF VALUE
cutoff_value = get_max_value(main, '_MODIFIED')
update_cutoff_value(cutoff_value, target_table, source_table)
update_run_datetime(run_datetime, target_table, source_table)
