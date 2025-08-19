# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run /notebooks/_SHARED/FUNCTIONS/1.2/func_data_validation

# COMMAND ----------

from datetime import datetime
from pyspark.sql import *
from pyspark.sql.functions import *

# INPUT PARAMETERS
database_name = get_input_param('database_name', default_value = 's_logility')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'list', default_value = ['Item','Location_DC','Source_Location','PlannedMonth','Production_Resources','GenerationDate','Average_Dependent_Demand'])
overwrite = get_input_param('overwrite', 'bool', default_value = False)
prune_days = get_input_param('prune_days', 'int', default_value = 1)
table_name = get_input_param('table_name', default_value = 'logility_planned_orders_summarised')
target_folder = get_input_param('target_folder', default_value = '/datalake_silver/logility/full_data')
temp_folder = get_input_param('temp_folder', default_value = '/datalake_silver/logility/temp_data')
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
run_datetime = currenttime.strftime('%Y-%m-%dT%H:%M:%S')
source = "LOG"
source_table = 'logftp.logility_sp_planned_orders_export'

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(source_table, None, prune_days)
#   cutoff_value = "2023-03-02"
  print(cutoff_value)
  source_df = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  source_df = load_full_dataset(source_table)

source_df.createOrReplaceTempView('logility_sp_planned_orders_export')

# COMMAND ----------

logftp_df = spark.sql(f"select *,last_day(Ship_Date) PlannedMonth, rank() over (partition by Item,Location_DC,Source_Location,Ship_Date order by Production_Resources) rnk from logility_sp_planned_orders_export")
logftp_df.createOrReplaceTempView('logftp_df')
# logftp_df.display()

# COMMAND ----------

main=spark.sql(f"""
SELECT 
  Item,
  Location_DC,
  Source_Location,  
  Production_Resources,  
  SUM(Quantity) Quantity,
  UOM,
  Planner_ID,
  Avg_Dep_Dmd AS Average_Dependent_Demand,
  Safety_Stock_Quantity,
  GenerationDate,
  PlannedMonth
FROM
logftp_df
WHERE PlannedMonth is not null
and rnk=1
GROUP BY
  Item,
  Location_DC,
  Source_Location,  
  Production_Resources,  
  UOM,
  Planner_ID,
  Avg_Dep_Dmd,
  Safety_Stock_Quantity,
  GenerationDate,
  PlannedMonth
""")

# main.display()
main.createOrReplaceTempView('PlannedOrders_Summary')
print(main.count())

# COMMAND ----------

register_hive_table(main, target_table, target_folder, options = {'overwrite': overwrite})
append_into_table(main, target_table, {"incremental_column":"GenerationDate"})

# COMMAND ----------

sqlq=f"DELETE FROM {target_table} WHERE GenerationDate <= add_months(last_day(current_date),-13)"
spark.sql(sqlq)

# COMMAND ----------

# max_value = get_max_value(main, "GenerationDate")
update_cutoff_value(run_datetime, target_table, source_table)
update_run_datetime(run_datetime, target_table, source_table)
