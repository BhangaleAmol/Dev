# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run /notebooks/_SHARED/FUNCTIONS/1.2/func_data_validation

# COMMAND ----------

from datetime import datetime

# INPUT PARAMETERS
database_name = get_input_param('database_name', default_value = 's_logility')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'list', default_value = ['Product','Location','generationDate'])
overwrite = get_input_param('overwrite', 'bool', default_value = False)
prune_days = get_input_param('prune_days', 'int', default_value = 1)
table_name = get_input_param('table_name', default_value = 'IP_SafetyStock_Summary')
target_folder = get_input_param('target_folder', default_value = '/datalake_silver/logility/full_data')
temp_folder = get_input_param('temp_folder', default_value = '/datalake_silver/logility/temp_data')
sampling = get_input_param('sampling', 'bool', default_value = False)
storage_name = get_input_param('storage_name',  default_value = 'edmans{env}data001')
file_extension = get_input_param('file_extension',  default_value = 'dlt')
container_name = get_input_param('container_name',  default_value = 'datalake')
file_name = table_name.lower() + '.' + file_extension

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
source = "LOG"
source_table = 'logftp.logility_ip_export_for_safetystock'
viewName = 'logility_ip_export_for_safetystock'
  
print(table_name,source_table)  

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(source_table, None, prune_days)
  print(cutoff_value)
  logility_ip_export_for_safetystock = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  logility_ip_export_for_safetystock = load_full_dataset(source_table)

logility_ip_export_for_safetystock.createOrReplaceTempView(viewName)

# COMMAND ----------

# DBTITLE 1,Forecast
from pyspark.sql.functions import lpad,col,concat,trunc,substring,last_day,add_months,to_date,lit,round,when

IDColumns = ["Lvl1Fcst","Lvl2Fcst","ASOFMN","ASOFYR"]
pivotColumns = ("AVSS")
includedColumns = ["Field-50","PRIMEABC","REPLLT1","ABCALT1","UNITCST","UNITPRC"]
FinalColList = ["Lvl1Fcst as Product","Lvl2Fcst as Location","`Field-50` as XYZCode","PRIMEABC","ABCALT1","REPLLT1 as Replenishment_Leadtime","SafetyStock_1_month_Units","SafetyStock_3_month_Units","SafetyStock_LeadTime_Units","SafetyStock_1_month_Cost","SafetyStock_3_month_Cost","SafetyStock_LeadTime_Cost","generationDate","createdOn","modifiedOn","createdBy","modifiedBy"]

pivot_cols = [x for x in logility_ip_export_for_safetystock.columns if x.strip().startswith(pivotColumns)]
excludeColumns = [x for x in logility_ip_export_for_safetystock.columns if not x.strip().startswith(pivotColumns) and x not in IDColumns and x not in includedColumns]
includedColumns = IDColumns + includedColumns +  pivot_cols
# print(pivot_cols)

pivoted_df = logility_ip_export_for_safetystock.select(includedColumns)

# Using static period numbers in the column names
pivoted_df=pivoted_df \
    .withColumn("MonthYear",lpad(concat(col('ASOFMN'),col('ASOFYR')),6,'0')) \
    .withColumn("AsOfMonthYear",trunc(to_date(concat(substring(col('MonthYear'), 1,2),lit("-01-"),substring(col('MonthYear'), 3,4)),'MM-dd-yyyy'),"month")) \
    .withColumn("generationDate",last_day(add_months(col("AsOfMonthYear"),1))) \
    .withColumn("LeadTime",round(col("REPLLT1")/30)) 

# calculate Monthly Units
pivoted_df=pivoted_df \
    .withColumn("SafetyStock_1_month_Units",col("AVSS1")) \
    .withColumn("SafetyStock_3_month_Units",col("AVSS3")) \
    .withColumn("SafetyStock_LeadTime_Units",
                when(pivoted_df.LeadTime==0,col("AVSS1")) \
                .when(pivoted_df.LeadTime==1,col("AVSS1")) \
                .when(pivoted_df.LeadTime==2,col("AVSS2")) \
                .when(pivoted_df.LeadTime==3,col("AVSS3")) \
                .when(pivoted_df.LeadTime==4,col("AVSS4")) \
                .when(pivoted_df.LeadTime==5,col("AVSS5")) \
                .when(pivoted_df.LeadTime==6,col("AVSS6")) \
                .when(pivoted_df.LeadTime==7,col("AVSS7")) \
                .when(pivoted_df.LeadTime==8,col("AVSS8")) \
                .when(pivoted_df.LeadTime==9,col("AVSS9")) \
                .when(pivoted_df.LeadTime==10,col("AVSS10")) \
                .when(pivoted_df.LeadTime==11,col("AVSS11")) \
                .when(pivoted_df.LeadTime==12,col("AVSS12"))                
               )

# calculate Monthly Costs                
main=pivoted_df \
    .withColumn("SafetyStock_1_month_Cost",round(col("SafetyStock_1_month_Units")*col("UNITCST"),4)) \
    .withColumn("SafetyStock_3_month_Cost",round(col("SafetyStock_3_month_Units")*col("UNITCST"),4)) \
    .withColumn("SafetyStock_LeadTime_Cost",round(col("SafetyStock_LeadTime_Units")*col("UNITCST"),4)) \
    .withColumn("createdOn",lit(currenttime)) \
    .withColumn("modifiedOn",lit(currenttime)) \
    .withColumn("createdBy",lit(currentuser)) \
    .withColumn("modifiedBy",lit(currentuser)) \
    .selectExpr(FinalColList)

main.display()
main.createOrReplaceTempView('vwSPSafetyStock') 

# COMMAND ----------

if incremental:
  register_hive_table(main, target_table, target_folder, options = {'overwrite': overwrite})
  merge_into_table(main, target_table, key_columns, options = {'auto_merge': True})  
else:  
  register_hive_table(main, target_table, target_folder, options = {'overwrite': overwrite})
  append_into_table(main, target_table,{"incremental_column":"generationDate"})  

# COMMAND ----------

# UPDATE CUTOFF VALUE
cutoff_value = get_max_value(main, 'modifiedOn')
update_cutoff_value(cutoff_value, target_table, source_table)
update_run_datetime(run_datetime, target_table, source_table)
