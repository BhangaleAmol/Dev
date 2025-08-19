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
key_columns = get_input_param('key_columns', 'list', default_value = ["PROD_CD","CHAN_ID"])
overwrite = get_input_param('overwrite', 'bool', default_value = False)
prune_days = get_input_param('prune_days', 'int', default_value = 30)
table_name = get_input_param('table_name', default_value = 'forecast_infordp')
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

data1 = [("831011","BDCHT-M"),
    ("831012","BDCHT-L"),
    ("831013","BDCHT-XL"),
    ("831015","BDCHT-XXXL"),
    ("835622","4-644-9"),
    ("835623","4-644-10"),
    ("842315","4576"),
    ("842316","4574"),
    ("842317","4572"),
    ("842318","4578"),
    ("842319","4570"),
    ("104092","74-045-9"),
    ("104396","74-047-7"),
    ("104397","74-047-8"),
    ("104398","74-047-9"),
    ("104399","74-047-10"),
    ("162839","MF-300-L"),
    ("162840","MF-300-M"),
    ("162844","MF-300-XL"),
    ("162845","MF-300-XS"),
    ("163164","SG-375-L"),
    ("163165","SG-375-M"),
    ("163166","SG-375-S"),
    ("163168","SG-375-XL"),
    ("284107","48-305-8"),
    ("284108","48-305-9"),
    ("284200","48-125-6"),
    ("284201","48-125-7"),
    ("284202","48-125-8"),
    ("284203","48-125-9"),
    ("284204","48-125-10"),
    ("105131","105131"),
    ("105132","105132"),
    ("105133","105133"),
    ("284380","48-501-8"),
    ("284381","48-501-9"),
    ("284382","48-501-10"),
    ("288546","58-530-8"),
    ("288547","58-530-9"),
    ("288548","58-530-10"),
    ("288549","58-535-8"),
    ("288550","58-535-9"),
    ("288551","58-535-10"),         
    ("502652","4520N"),
    ("502653","4524N"),
    ("502654","4526N"),
    ("588104","92-605-S"),
    ("588105","92-605-M"),
    ("800233","92-670-M"),
    ("800234","92-670-L"),         
    ("816874","354105"),
    ("816875","354106"),
    ("816876","354107"),
    ("816878","354101"),
    ("816879","354102"),
    ("816880","354103"),
    ("821150","70-205-8"),
    ("821321","32-105-8"),
    ("821322","32-105-9"),
    ("821325","32-125-7.5"),
    ("821326","32-125-8"),
    ("827971","48101RH090"),
    ("829859","BCAH-CL"),
    ("830648","93843080"),
    ("830816","92-220-S"),
    ("830827","92-481A-L"),
    ("200921024","200920124"),
    ("200921025","200920125")
  ]

schema1 = StructType([ \
    StructField("e2eProductCode",StringType(),True), \
    StructField("productcode",StringType(),True)  
  ])
 
df = spark.createDataFrame(data=data1,schema=schema1)
df.createOrReplaceTempView('productcode_mapping')

data2 = [("DC819","819-CO","EBS"),
    ("DCAAL1","AAL1-NW","SAP"),
    ("DCAAL2","AAL2-W","SAP"),
    ("DCAAL3","AAL3-K1","SAP"),
    ("DCABL","520-ABL","TOT"),         
    ("DCAJN1","AJN1-JP","SAP"),
    ("DCANV1","20-11","SAP"),
    ("DCANV2","20-13","SAP"),
    ("DCANV3","20-14","SAP"),
    ("DCHER","521-HER","TOT"),
    ("DCASWH","KGD-CH","KGD"),
    ("DCNLT1","NLT1-WH","SAP"),
    ("DCAMR1","87-15","SAP"),
    ("ANZ_DSAPAC_DS","ANZ-DS","SAP"),
    ("IND_DSAPAC_DS","724-DS","EBS"),
    ("KOR_DSAPAC_DS","327-XD","EBS"),
    ("ROA_DSAPAC_DS","327-XD","EBS"),
    ("JP_DSAPAC_DS","327-XD","EBS"),
    ("CHN_DSAPAC_DS","200-XD","EBS"),
    ("DSAPAC","327-XD","EBS"),
    ("CA_DSNA_DS","400-DS","EBS"),
    ("DSEMEA","EUR-DS","SAP"),
    ("DSLAC","518-DS","EBS"),
    ("US_DSNA_DS","800-DS","EBS")
  ]

schema2 = StructType([ \
    StructField("forecast2Id",StringType(),True), \
    StructField("CHAN_ID",StringType(),True), \
    StructField("_SOURCE",StringType(),True)   
  ])
 
df = spark.createDataFrame(data=data2,schema=schema2)
df.createOrReplaceTempView('infor_dp_channel')

# COMMAND ----------

query = f"""
          WITH cte
(
       select forecast1id,
              CASE
                     WHEN substr(forecast2id,charindex('_',forecast2id)+1,length(forecast2id)-charindex('_',forecast2id)-3) = 'DSAPAC' THEN
                            CASE
                                   WHEN instr(forecast2id,'ANZ_DSAPAC_DS') = 1 THEN 'ANZ_DSAPAC_DS'
                                   WHEN instr(forecast2id,'ROA_DSAPAC_DS') = 1 THEN 'ROA_DSAPAC_DS'
                                   WHEN instr(forecast2id,'KOR_DSAPAC_DS') = 1 THEN 'KOR_DSAPAC_DS'
                                   WHEN instr(forecast2id,'JP_DSAPAC_DS')  = 1 THEN 'JP_DSAPAC_DS'
                                   WHEN instr(forecast2id,'CHN_DSAPAC_DS') = 1 THEN 'CHN_DSAPAC_DS'
                                   WHEN instr(forecast2id,'IND_DSAPAC_DS') = 1 THEN 'IND_DSAPAC_DS'
                                   ELSE substr(forecast2id,charindex('_',forecast2id)+1,length(forecast2id)-charindex('_',forecast2id)-3)
                            END
                     WHEN substr(forecast2id,charindex('_',forecast2id)+1,length(forecast2id)-charindex('_',forecast2id)-3) = 'DSNA' THEN
                            CASE
                                   WHEN instr(forecast2id,'US_DSNA_DS') = 1 THEN 'US_DSNA_DS'
                                   WHEN instr(forecast2id,'CA_DSNA_DS') = 1 THEN 'CA_DSNA_DS'
                                   ELSE substr(forecast2id,charindex('_',forecast2id)+1,length(forecast2id)-charindex('_',forecast2id)-3)
                            END
                     ELSE substr(forecast2id,charindex('_',forecast2id)+1,length(forecast2id)-charindex('_',forecast2id)-3)
              END                                         AS forecast2id,
              cast(resultantnetforecast AS numeric(12,1)) AS resultantnetforecast,
              forecastmonth,
              date_format(forecastmonth,'yyyy-MM-dd') AS strforecastmonth,
              pdh.gbu 
       FROM   s_logility.doforecast fo
       LEFT JOIN pdh.master_records pdh on pdh.item = fo.forecast1Id
       WHERE  generationdate = '{strRunperiodDate}'
       AND    forecastmonth BETWEEN '{strRunperiodDate}' AND    add_months('{strRunperiodDate}',23) 
), 
final
(
          SELECT    forecast1id,
                    CASE
                              WHEN pc.productcode IS NULL THEN COALESCE(pr.productcode,forecast1id)
                              ELSE pc.productcode
                    END           PROD_CD,
                    CASE WHEN cte.forecast2id = 'DCANV1' and cte.gbu <> 'MEDICAL' THEN '20-11' 
                         WHEN cte.forecast2id = 'DCANV1' and cte.gbu = 'MEDICAL' THEN '20-12'
                    ELSE ch.chan_id END AS CHAN_ID,
                    resultantnetforecast,
                    forecastmonth,
                    date_format(forecastmonth,'yyyy-MM-dd')                                       AS strforecastmonth,
                    rank() OVER (partition BY cte.forecast1id,pr._SOURCE ORDER BY pr.productcode) AS rnk
          FROM      cte
          JOIN      infor_dp_channel ch
          ON        cte.forecast2id = ch.forecast2id
          LEFT JOIN s_core.product_agg pr
          ON        pr.e2eproductcode = cte.forecast1id
          AND       pr._SOURCE=ch._SOURCE AND upper(pr.productStatus) = 'ACTIVE'
          LEFT JOIN productcode_mapping pc
          ON        cte.forecast1id = pc.e2eproductcode
          AND       ch._SOURCE='KGD' 
)
SELECT   PROD_CD,
         CHAN_ID,
         Sum(resultantnetforecast) AS resultantNetForecast,
         strforecastmonth
FROM     final
WHERE    rnk=1
GROUP BY PROD_CD,
         CHAN_ID,
         strforecastmonth
ORDER BY 4
"""
print(query)
forecast_infordp = spark.sql(query)
# forecast_infordp.createOrReplaceTempView('forecast_infordp')
# forecast_infordp.display()

# COMMAND ----------

# DBTITLE 1,Forecast ASCP
IDCols = ["PROD_CD","CHAN_ID"]

main = forecast_infordp.groupBy(IDCols) \
.pivot("strforecastMonth") \
.agg(max(col("resultantNetForecast"))) \

#order columns by date values
dateList = [x for x in main.columns if x not in IDCols]
dateList.sort(key=lambda date: datetime.strptime(date, "%Y-%m-%d"))

main = main.select(IDCols + dateList)
# main.display()
# main.createOrReplaceTempView('main')

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
