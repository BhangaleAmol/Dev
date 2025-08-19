# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run /notebooks/_SHARED/FUNCTIONS/1.2/func_data_validation

# COMMAND ----------

from datetime import datetime
from pyspark.sql import *
from pyspark.sql.functions import *

# INPUT PARAMETERS
database_name = get_input_param('database_name', default_value = 'g_logility')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'list', default_value = ['_ID'])
overwrite = get_input_param('overwrite', 'bool', default_value = False)
prune_days = get_input_param('prune_days', 'int', default_value = 7)
table_name = get_input_param('table_name', default_value = 'forecastaccuracy')
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
source = "LOG"
source_table = "s_logility.DODemand"

if table_name.startswith('LVL3_') or table_name.startswith('lvl3_'):
  forecast_table = 's_logility.Lvl3_DOForecast'
  demand_table = 's_logility.Lvl3_DODemand'
  viewName = 'Lvl3_DOForecastAccuracy'
else:
  forecast_table = 's_logility.DOForecast'
  demand_table = 's_logility.DODemand'
  viewName = 'DOForecastAccuracy'
  
print(forecast_table,demand_table,viewName)

# COMMAND ----------

# DBTITLE 1,Determine current runPeriod
from dateutil.relativedelta import relativedelta
from pandas.tseries.offsets import MonthEnd
import pandas as pd
import os

runperiod=get_run_period()
# runperiod='202303'
runperioddate = (pd.to_datetime(runperiod, format="%Y%m") - relativedelta(months=0)) + MonthEnd(1)
strRunperiodDate = runperioddate.strftime("%Y-%m-%d")
fromDate = runperioddate - relativedelta(months=4)
toDate = runperioddate - relativedelta(months=1)
print(strRunperiodDate,fromDate.strftime("%Y-%m-%d"),toDate.strftime("%Y-%m-%d"))
lst=[(pd.to_datetime(x, format="%Y-%m-%d") + MonthEnd(1)).strftime("%Y-%m-%d") for x in pd.date_range(fromDate,toDate, freq='MS')]
strPeriod = ", ".join(["'{}'".format(value) for value in lst])
print(strPeriod)

# COMMAND ----------

# DBTITLE 1,Combine Forecast & Demand
if not incremental:
  query = f"""
WITH cteforecast
(
       select forecast1id,
              forecast2id,
              forecast3id,
              resultantnetforecast,
              AD3FForecast,
              UAF3Forecast,
              generationdate AS forecastgd,
              forecastmonth  AS fm
       FROM   {forecast_table}
)
, ctedemand
(
         SELECT   forecast1id,
                  forecast2id,
                  forecast3id,
                  generationdate,
                  forecastmonth     AS fm,
                  sum(actualdemand)    actualdemand
         FROM     {demand_table}
         WHERE    actualdemand IS NOT NULL
         GROUP BY forecast1id,
                  forecast2id,
                  forecast3id,
                  generationdate,
                  forecastmonth 
)
, cte
(
                SELECT DISTINCT fo.forecast1id,
                                fo.forecast2id,
                                fo.forecast3id,
                                fo.resultantnetforecast,
                                fo.AD3FForecast,
                                fo.UAF3Forecast,
                                fo.forecastgd                                                                                                 AS forecastgd,
                                fo.fm                                                                                                         AS forecastmonth,
                                concat('forecastMonth_',cast(int(months_between(do.generationdate,fo.forecastgd)) AS string))                 AS previousmonths,
                                do.actualdemand,
                                do.generationdate AS generationdate
                FROM            cteforecast fo
                JOIN            ctedemand do
                ON              fo.forecast1id =do.forecast1id
                AND             fo.forecast2id = do.forecast2id
                AND             fo.forecast3id = do.forecast3id
                WHERE           fo.fm = do.generationdate
                AND             do.generationdate = do.fm
                AND             actualdemand IS NOT NULL 
)
SELECT * FROM cte
"""
else:
  query = f"""
WITH cteforecast
(
       select forecast1id,
              forecast2id,
              forecast3id,
              resultantnetforecast,
              AD3FForecast,
              UAF3Forecast,
              generationdate                                           AS forecastgd,
              forecastmonth                                            AS fm ,
              int(months_between('{strRunperiodDate}',generationdate)) AS previousmonths
       FROM   {forecast_table}
       WHERE  generationdate IN ({strPeriod}) 
)
, ctedemand
(
         SELECT   forecast1id,
                  forecast2id,
                  forecast3id,
                  generationdate,
                  forecastmonth     AS fm,
                  sum(actualdemand)    actualdemand
         FROM     {demand_table}
         WHERE    actualdemand IS NOT NULL
         AND      generationdate = '{strRunperiodDate}'
         GROUP BY forecast1id,
                  forecast2id,
                  forecast3id,
                  generationdate,
                  forecastmonth 
)
, cte
(
                SELECT DISTINCT fo.forecast1id,
                                fo.forecast2id,
                                fo.forecast3id,
                                fo.resultantnetforecast,
                                fo.AD3FForecast,
                                fo.UAF3Forecast,
                                fo.forecastgd                                                              AS forecastgd,
                                fo.fm                                                                      AS forecastmonth,
                                concat('forecastMonth_',cast(fo.previousmonths AS string))                 AS previousmonths,
                                do.actualdemand,
                                do.generationdate AS generationdate
                FROM            cteforecast fo
                JOIN            ctedemand do
                ON              fo.forecast1id =do.forecast1id
                AND             fo.forecast2id = do.forecast2id
                AND             fo.forecast3id = do.forecast3id
                WHERE           fo.fm = do.generationdate
                AND             do.generationdate = do.fm
                AND             fo.fm=do.fm
                AND             actualdemand IS NOT NULL 
)
SELECT * FROM cte
"""
  
print(query)
logility_do_forecast = spark.sql(query)
# logility_do_forecast.display()

# COMMAND ----------

# DBTITLE 1,Build Forecast Accuracy
FinalColList = ["_ID","forecast1Id","forecast2Id","forecast3Id","generationDate","actualDemand"]

pivoted_df = logility_do_forecast.groupBy("forecast1Id","forecast2Id","forecast3Id","generationDate","actualDemand").pivot("previousmonths").agg(avg("resultantNetForecast").alias("avg_resultantNetForecast"),avg("AD3FForecast").alias("avg_AD3FForecast"),avg("UAF3Forecast").alias("avg_UAF3Forecast")) \
.withColumn("_SOURCE",lit(source)) \
.withColumn("_ID",sha2(concat_ws("||","forecast1Id","forecast2Id","forecast3Id","generationDate","_SOURCE"),256)) \
.withColumn("generationDate_1",last_day(add_months(col("generationDate"),-1))) \
.withColumn("generationDate_2",last_day(add_months(col("generationDate"),-2))) \
.withColumn("generationDate_3",last_day(add_months(col("generationDate"),-3)))

# pivoted_df.display()
#Adding RSLF coulmns
for colname in pivoted_df.columns:
  if colname == "forecastMonth_1_avg_resultantNetForecast":
    FinalColList.append("NetForecast_1")
    pivoted_df = pivoted_df.withColumn("NetForecast_1",col("forecastMonth_1_avg_resultantNetForecast").cast("numeric(12,2)"))
  if colname == "forecastMonth_2_avg_resultantNetForecast":
    FinalColList.append("NetForecast_2")
    pivoted_df = pivoted_df.withColumn("NetForecast_2",col("forecastMonth_2_avg_resultantNetForecast").cast("numeric(12,2)"))
  if colname == "forecastMonth_3_avg_resultantNetForecast":
    FinalColList.append("NetForecast_3")
    pivoted_df = pivoted_df.withColumn("NetForecast_3",col("forecastMonth_3_avg_resultantNetForecast").cast("numeric(12,2)"))

for colname in pivoted_df.columns:    
  if colname == "NetForecast_1":
    FinalColList.append("generationDate_1")
  if colname == "NetForecast_2":
    FinalColList.append("generationDate_2")
  if colname == "NetForecast_3":
    FinalColList.append("generationDate_3")
    
# # Adding AD3F columns    
for colname in pivoted_df.columns:
  if colname == "forecastMonth_1_avg_AD3FForecast":
    FinalColList.append("AD3FForecast_1")
    pivoted_df = pivoted_df.withColumn("AD3FForecast_1",col("forecastMonth_1_avg_AD3FForecast").cast("numeric(12,2)"))
  if colname == "forecastMonth_2_avg_AD3FForecast":
    FinalColList.append("AD3FForecast_2")
    pivoted_df = pivoted_df.withColumn("AD3FForecast_2",col("forecastMonth_2_avg_AD3FForecast").cast("numeric(12,2)"))
  if colname == "forecastMonth_3_avg_AD3FForecast":
    FinalColList.append("AD3FForecast_3")
    pivoted_df = pivoted_df.withColumn("AD3FForecast_3",col("forecastMonth_3_avg_AD3FForecast").cast("numeric(12,2)"))

# # Adding UAF3 columns    
for colname in pivoted_df.columns:
  if colname == "forecastMonth_1_avg_UAF3Forecast":
    FinalColList.append("UAF3Forecast_1")
    pivoted_df = pivoted_df.withColumn("UAF3Forecast_1",col("forecastMonth_1_avg_UAF3Forecast").cast("numeric(12,2)"))
  if colname == "forecastMonth_2_avg_UAF3Forecast":
    FinalColList.append("UAF3Forecast_2")
    pivoted_df = pivoted_df.withColumn("UAF3Forecast_2",col("forecastMonth_2_avg_UAF3Forecast").cast("numeric(12,2)"))
  if colname == "forecastMonth_3_avg_UAF3Forecast":
    FinalColList.append("UAF3Forecast_3")
    pivoted_df = pivoted_df.withColumn("UAF3Forecast_3",col("forecastMonth_3_avg_UAF3Forecast").cast("numeric(12,2)"))   

main=pivoted_df.select(FinalColList) \
.withColumn("createdOn",lit(currenttime)) \
.withColumn("modifiedOn",lit(currenttime)) \
.withColumn("createdBy",lit(currentuser)) \
.withColumn("modifiedBy",lit(currentuser)) \

main.createOrReplaceTempView(viewName)
# main.display()

# COMMAND ----------

# VALIDATE DATA
valid_count_rows(main, key_columns)

# COMMAND ----------

if incremental:
  register_hive_table(main, target_table, target_folder, options = {'overwrite': overwrite, 'partition_column':'generationDate'})
  merge_into_table(main, target_table, key_columns)
else:
  table_exist = table_exists(target_table) 
  if not table_exist:
    register_hive_table(main, target_table, target_folder, options = {'overwrite': True, 'partition_column':'generationDate'})
 
  location = get_table_location(target_table)
  main.write \
    .format("delta") \
    .mode("overwrite") \
    .option("path", location) \
    .option("overwriteSchema", "true")\
    .saveAsTable(table_name)

# COMMAND ----------

# UPDATE CUTOFF VALUE
cutoff_value = get_max_value(main, 'modifiedOn')
update_cutoff_value(cutoff_value, target_table, source_table)
update_run_datetime(run_datetime, target_table, source_table)
