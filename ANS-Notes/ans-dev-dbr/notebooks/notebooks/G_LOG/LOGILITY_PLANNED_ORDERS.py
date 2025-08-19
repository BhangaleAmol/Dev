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
key_columns = get_input_param('key_columns', 'list', default_value = ['Product_Code','UOM','Source_Location','Location_DC','Current_Month','Lines','Planner_ID','Avg_Dep_Dmd'])
overwrite = get_input_param('overwrite', 'bool', default_value = False)
prune_days = get_input_param('prune_days', 'int', default_value = 30)
table_name = get_input_param('table_name', default_value = 'logility_planned_orders')
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
source_table = 's_logility.logility_planned_orders_summarised'

# COMMAND ----------

from dateutil.relativedelta import relativedelta
from pandas.tseries.offsets import MonthEnd
import pandas as pd
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
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

# this table is loaded only on first 2 satrudays of the month
source_df = spark.sql(f"""
with first2saturdays
(
select date_add(date_trunc('WEEK', trunc(current_date, 'month')),5) firstsat
union
select date_add(date_trunc('WEEK', trunc(current_date, 'month')),12)
),
cte (
select *,concat('m+',int(months_between(PlannedMOnth,GenerationDate))) as Monthsbw,last_day(generationdate) monthend from {source_table} where generationdate = (select max(generationdate) mxg from {source_table})
)
select 
Item,
Location_DC,
Source_Location,
Production_Resources,
Quantity,
UOM,
Planner_ID,
Average_Dependent_Demand,
Safety_Stock_Quantity,
PlannedMonth,
Monthsbw,
GenerationDate,
MonthEnd
from cte  
where
GenerationDate in 
(select firstsat from first2saturdays)
""")
source_df.createOrReplaceTempView('logility_sp_planned_orders')
source_df.display()
rowcount=source_df.count()
print(rowcount)

# COMMAND ----------

if rowcount > 0:
  IDCols = ["Item","Location_DC"]

  dfQuantity = source_df.groupBy(IDCols) \
  .pivot("Monthsbw") \
  .agg(max(col("Quantity"))) \

  dfQuantity.createOrReplaceTempView('dfQuantity')
#   dfQuantity.display()

# code needed if we want full date values as column names
#order columns by date values
# dateList = [x for x in dfQuantity.columns if x not in IDCols]
# print(dateList)
# dateList.sort(key=lambda date: datetime.strptime(date, "%Y-%m-%d"))
# strdateList="`" + "`,`".join(dateList) + "`"
# print(strdateList)
# dfQuantity = dfQuantity.select(IDCols + dateList)


# COMMAND ----------

if rowcount > 0:
  main=spark.sql(f"""
  WITH cte_demand
  (
           select   forecast1id,
                    substr(forecast2id,charindex('_',forecast2id)+1,length(forecast2id)-charindex('_',forecast2id)-3) AS dc,
                    cast(sum(actualdemand)/3 AS numeric(10,2))                                                          AS average_demand
           FROM     s_logility.dodemand
           WHERE    forecastMonth BETWEEN last_day(add_months('{strRunperiodDate}',-3)) AND last_day(add_months('{strRunperiodDate}',-1))
           and generationdate = '{strRunperiodDate}'
           GROUP BY forecast1id,
                    substr(forecast2id,charindex('_',forecast2id)+1,length(forecast2id)-charindex('_',forecast2id)-3) ) 
  ,cte_forecast
  (
           SELECT   forecast1id,
                    substr(forecast2id,charindex('_',forecast2id)+1,length(forecast2id)-charindex('_',forecast2id)-3) AS dc,
                    cast(sum(resultantnetforecast)/3 AS numeric(10,2))                                                  AS average_forecast
           FROM     s_logility.doforecast
           WHERE    forecastMonth BETWEEN last_day(add_months('{strRunperiodDate}',+1)) AND      last_day(add_months('{strRunperiodDate}',+3))
           and generationdate = '{strRunperiodDate}'
           GROUP BY forecast1id,
                    substr(forecast2id,charindex('_',forecast2id)+1,length(forecast2id)-charindex('_',forecast2id)-3) ) 
  ,cte_cost
  (
           SELECT   forecast_1_id,
                    substr(forecast_2_id,charindex('_',forecast_2_id)+1,length(forecast_2_id)-charindex('_',forecast_2_id)-3) AS dc,
                    cast(avg(unit_cost) AS numeric(25,15))                                                                    AS cost_price
           FROM     g_tembo.lom_aa_do_l1_hist_archive
           GROUP BY forecast_1_id,
                    substr(forecast_2_id,charindex('_',forecast_2_id)+1,length(forecast_2_id)-charindex('_',forecast_2_id)-3) 
  )
  SELECT DISTINCT loc.title                     AS Region,
                  loc.locationabbreviationvalue AS Type,
                  pdh.external_product_id       AS Local_Code,
                  Source_Location,
                  GBU,
                  Style_Code,
                  replace(po.Location_DC,'DC','') as Location_DC,
                  pdh.Description,
                  pdh.Size,
                  po.item AS Product_Code,
                  po.UOM,
                  cost.Cost_Price,
                  dem.Average_Demand,
                  fc.Average_Forecast,                
                  Average_Dependent_Demand as Avg_Dep_Dmd,
                  cdr.on_hand_quantity    AS Stock_on_Hand,
                  cdr.in_transit_quantity AS In_Transit,
                  cdr.on_order_quantity   AS On_Order,
                  '{strRunperiodDate}'    AS Current_Month,
                  po.Planner_ID,
                  po.Safety_Stock_Quantity,
                  dfquantity.`m+1`,
                  dfquantity.`m+2`,
                  dfquantity.`m+3`,
                  dfquantity.`m+4`,
                  dfquantity.`m+5`,
                  dfquantity.`m+6`,
                  dfquantity.`m+7`,
                  dfquantity.`m+8`,
                  dfquantity.`m+9`,
                  dfquantity.`m+10`,
                  dfquantity.`m+11`,
                  dfquantity.`m+12`,
                  pdh.Dipping_Plant,
                  po.`production_resources`     AS Lines,
                  pdh.Knitting_Plant,
                  pdh.Base_Product
  FROM            logility_sp_planned_orders po
  LEFT JOIN       pdh.master_records pdh ON pdh.item = po.item
  LEFT JOIN       spt.locationmaster loc ON loc.globallocationcode = po.location_dc
  LEFT JOIN       cte_demand dem ON dem.forecast1id = po.item AND dem.dc = po.location_dc
  LEFT JOIN       cte_forecast fc ON fc.forecast1id = po.item AND fc.dc = po.location_dc
  LEFT JOIN       cte_cost cost ON cost.forecast_1_id = po.item AND cost.dc = po.location_dc
  LEFT JOIN       g_tembo.ip_cdr_as_l2 cdr ON cdr.forecast_1_id = po.item AND cdr.forecast_2_id = po.location_dc
  LEFT JOIN       dfquantity ON dfquantity.item = po.item AND dfquantity.location_dc = po.location_dc  
  """)  

#   main.display()
  main.createOrReplaceTempView('PlannedOrders')  

# COMMAND ----------

if rowcount > 0:  
  table_exist = table_exists(target_table) 
  if not table_exist:
    register_hive_table(main, target_table, target_folder, options = {'overwrite': True})
 
  location = get_table_location(target_table)  
  main.write \
    .format("delta") \
    .mode("overwrite") \
    .option("path", location) \
    .option("overwriteSchema", "true")\
    .saveAsTable(table_name)
