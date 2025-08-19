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
key_columns = get_input_param('key_columns', 'list', default_value = ["Inventory_Item_Number","Forecast_Designator","Organization"])
overwrite = get_input_param('overwrite', 'bool', default_value = True)
prune_days = get_input_param('prune_days', 'int', default_value = 30)
table_name = get_input_param('table_name', default_value = 'forecast_oracle_ascp')
target_folder = get_input_param('target_folder', default_value = '/datalake_gold/logility/full_data')
temp_folder = get_input_param('temp_folder', default_value = '/datalake_gold/logility/temp_data')
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
source_table = "s_logility.DOForecast"

# COMMAND ----------

# DBTITLE 1,Determine current runPeriod
from dateutil.relativedelta import relativedelta
from pandas.tseries.offsets import MonthEnd
import pandas as pd
import os

runperiod=get_run_period()
# runperiod='202212'
runperioddate = (pd.to_datetime(runperiod, format="%Y%m") - relativedelta(months=0)) + MonthEnd(1)
strRunperiodDate = runperioddate.strftime("%Y-%m-%d")
fromDate = runperioddate - relativedelta(months=4)
toDate = runperioddate - relativedelta(months=1)
print(strRunperiodDate,fromDate.strftime("%Y-%m-%d"),toDate.strftime("%Y-%m-%d"))
lst=[(pd.to_datetime(x, format="%Y-%m-%d") + MonthEnd(1)).strftime("%Y-%m-%d") for x in pd.date_range(fromDate,toDate, freq='MS')]
strPeriod = ", ".join(["'{}'".format(value) for value in lst])
print(strPeriod)

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(target_table, source_table, prune_days)
  print(cutoff_value)

# COMMAND ----------

query = f"""
          WITH cte
(
       select forecast1id                                                                                          AS Inventory_Item_Number,
              'I'                                                                                                  AS Forecast_Designator,
              substr(forecast2id,charindex('_',forecast2id)+1,length(forecast2id)-charindex('_',forecast2id)-3)    AS Organization,
              cast(resultantnetforecast                                          / ansstduomconv AS numeric(12,1)) AS ResultantNetforecast,
              forecastmonth,
              date_format(forecastmonth,'yyyy-MM-dd') AS strforecastmonth
       FROM   s_logility.doforecast fo
       JOIN
              (
                     SELECT e2eproductcode,
                            ansstduomconv,
                            CASE
                                   WHEN ansstduom='PR' THEN 'PAIR'
                                   WHEN ansstduom='PC' THEN 'PIECE'
                                   ELSE ansstduom
                            END AS uom
                     FROM   s_core.product_agg
                     WHERE  _source="EBS") pr
       ON     pr.e2eproductcode = fo.forecast1id
       AND    pr.uom = fo.uom
       WHERE  generationdate = '{strRunperiodDate}'
       AND    forecastmonth BETWEEN '{strRunperiodDate}' AND    add_months('{strRunperiodDate}',23) 
) 
SELECT    Inventory_Item_Number,
          Forecast_Designator,
          Organization,
          sum(resultantnetforecast) ResultantNetforecast,
          ForecastMonth,
          strforecastmonth
       FROM      cte
       LEFT JOIN spt.locationmaster loc  ON loc.globallocationcode = cte.organization       
       WHERE     organization IN ('DC326',
                                  'DC327',
                                  'DC400',
                                  'DC403',
                                  'DC514',
                                  'DC518',
                                  'DC522',
                                  'DC724',
                                  'DC802',
                                  'DC803',
                                  'DC804',
                                  'DC809',
                                  'DC810',
                                  'DC813',
                                  'DC814',
                                  'DC822',
                                  'DC823',
                                  'DC826',
                                  'DC827',
                                  'DC828')
          AND       resultantnetforecast <> 0
          AND       upper(loc.statusvalue) <> 'INACTIVE'
          GROUP BY  Inventory_Item_Number,
                    Forecast_Designator,
                    Organization,
                    ForecastMonth,
                    strforecastmonth
"""
print(query)
forecast_ascp = spark.sql(query)
# forecast_ascp.display()

# COMMAND ----------

# DBTITLE 1,Forecast ASCP
IDCols = ["Inventory_Item_Number","Forecast_Designator","Organization"]

main = forecast_ascp.groupBy(IDCols) \
.pivot("strforecastMonth") \
.agg(max(col("resultantNetForecast"))) \

#order columns by date values
dateList = [x for x in main.columns if x not in IDCols]
dateList.sort(key=lambda date: datetime.strptime(date, "%Y-%m-%d"))

main = main \
  .na.fill(value=0,subset=dateList) \
  .select(IDCols + dateList)

# main.display()

# COMMAND ----------

table_exist = table_exists(target_table) 
if not table_exist:
  register_hive_table(main, target_table, target_folder, options = {'overwrite': True})
table_path = get_file_path(target_folder, file_name, container_name, storage_name)
print(table_path)
main.write \
    .format("delta") \
    .mode("overwrite") \
    .option("path", table_path) \
    .option("overwriteSchema", "true")\
    .saveAsTable(table_name)
