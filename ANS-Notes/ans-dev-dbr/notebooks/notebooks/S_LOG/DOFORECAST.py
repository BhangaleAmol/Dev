# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run /notebooks/_SHARED/FUNCTIONS/1.2/func_data_validation

# COMMAND ----------

from datetime import datetime

# INPUT PARAMETERS
database_name = get_input_param('database_name', default_value = 's_logility')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'list', default_value = ['_ID'])
overwrite = get_input_param('overwrite', 'bool', default_value = False)
prune_days = get_input_param('prune_days', 'int', default_value = 5)
table_name = get_input_param('table_name', default_value = 'doforecast')
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
source = "LOG"

if table_name.startswith('LVL3_'):
  source_table = 'logftp.logility_do_lvl3_forecast_export_for_dea_refresh'
  viewName = 'logility_do_lvl3_forecast_export_for_dea_refresh'
else:
  source_table = 'logftp.logility_do_forecast_export_for_dea_refresh'
  viewName = 'logility_do_forecast_export_for_dea_refresh'
  
print(table_name,source_table)  

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(target_table, source_table, prune_days)  
  print(cutoff_value)
  logility_do_forecast_export_for_dea_refresh = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  logility_do_forecast_export_for_dea_refresh = load_full_dataset(source_table)

logility_do_forecast_export_for_dea_refresh.createOrReplaceTempView(viewName)

# COMMAND ----------

# checking if the file is already loaded then dont load it again and set back the max to the previous value
if incremental:
  rowscount=logility_do_forecast_export_for_dea_refresh.count()
  sql=f"select if(count(*)>0, true, false) as isFileLoaded from {source_table} where _SOURCEFILENAME IN (select distinct _SOURCEFILENAME from {viewName})"
  df = spark.sql(sql)
  isFileLoaded = df.collect()[0][0]
  print(rowscount,isFileLoaded)
  
  if isFileLoaded or rowscount==0:
    dbutils.notebook.exit(json.dumps({"rowscount": rowscount}))

# COMMAND ----------

# DBTITLE 1,Rename columns function
from pyspark.sql import *
from pyspark.sql.functions import *

def rename_columns(df, columns):
    if isinstance(columns, dict):
        return df.select(*[col(col_name).alias(columns.get(col_name, col_name)) for col_name in df.columns])
    else:
        raise ValueError("'columns' should be a dict, like {'old_name_1':'new_name_1', 'old_name_2':'new_name_2'}")

# COMMAND ----------

# DBTITLE 1,Forecast
IDColumns = ["FCSTLVL","Lvl1Fcst","Lvl2Fcst","Lvl3Fcst","ASOFMN","ASOFYR"]
key_column = "JoinID"
pivotColumns = ("RSLF","AD1F","SYSF","FBGT","FBGD","AD3F","UAF3","MFCI")
excludeColumns = ["ENTIREKEY","ADRLTHLD1","ADRLTHLD2","FUTSYSHLD1","FUTSYSHLD2"]
FinalColList = ["_ID","forecastLevel","forecast1Id","forecast2Id","forecast3Id","uom","forecastModelType","StatLVLComp","StatTrendComp","StatSeasonal","StatLVLValue","StatTrendValue","MovingAvg","StatMAD","StatError","StatResSdv","StatSysSdv","Product_Life_Cycle","unForceStatisticalForecast","resultantNetForecast","futureBudgetUnits","futureBudgetValue","ADS1Forecast","AD3FForecast","UAF3Forecast","MFCIForecast","RSLF.generationDate","RSLF.forecastMonth","monthsLag","PrimeABC","PredecessorCode","SuccessorCode","SuccessorBeginDate","DemandVariability","_SOURCE","_SOURCEFILENAME","createdOn","modifiedOn","createdBy","modifiedBy"]
dict_renamecol = {'FCSTLVL':'forecastLevel', 'Lvl1Fcst':'forecast1Id', 'Lvl2Fcst':'forecast2Id', 'Lvl3Fcst':'forecast3Id', 'FCMODEL':'forecastModelType', 'PCALPHA':'statLVLComp', 'TCALPHA':'statTrendComp', 'SEAFCTAL':'statSeasonal', 'PERMCOMP':'statLVLValue', 'TRNDCOMP':'statTrendValue', 'MVAVGPER':'movingAvg', 'Field-47':'Product_Life_Cycle', 'SYSSMOOMAD':'statMAD', 'SMOO':'statError', 'RESSDV':'statResSdv', 'SYSSDV':'statSysSdv', 'PRIMEABC':'PrimeABC', 'Field-20':'PredecessorCode', 'Field-21':'SuccessorCode', 'Field-22':'SuccessorBeginDate', 'Field-50':'DemandVariability'} 

logility_df=logility_do_forecast_export_for_dea_refresh.withColumn("JoinID",sha2(concat_ws("||",array(IDColumns)),256))
pivot_cols = [x for x in logility_df.columns if x.strip().startswith(pivotColumns)]
unpivot_cols = [x for x in logility_df.columns if not x.strip().startswith(pivotColumns)]
unpivot_cols = [x for x in unpivot_cols if x not in excludeColumns]

pivotColExpr = " ,".join([f"'{c}', {c}" for c in pivot_cols])
pivotExpr = f"stack({len(pivot_cols)}, {pivotColExpr}) as (Type,value)"
pivoted_df = logility_df.select("JoinID","ASOFMN","ASOFYR",expr(pivotExpr))

# pivoted_df = pivoted_df.withColumn("value",col("value").cast("numeric"))

# Using static period numbers in the column names
pivoted_df=pivoted_df \
    .withColumn("MonthYear",lpad(concat(col('ASOFMN'),col('ASOFYR')),6,'0')) \
    .withColumn("AsOfMonthYear",trunc(to_date(concat(substring(col('MonthYear'), 1,2),lit("-01-"),substring(col('MonthYear'), 3,4)),'MM-dd-yyyy'),"month")) \
    .withColumn("forecastMonth", 
                when(pivoted_df.Type.startswith('RSLF'),expr("last_day(add_months(AsOfMonthYear,regexp_replace(Type,'RSLF','')))")) \
                .when(pivoted_df.Type.startswith('MFCI'),expr("last_day(add_months(AsOfMonthYear,regexp_replace(Type,'MFCI','')))")) \
                .when(pivoted_df.Type.startswith('AD1F'),expr("last_day(add_months(AsOfMonthYear,regexp_replace(Type,'AD1F','')))")) \
                .when(pivoted_df.Type.startswith('SYSF'),expr("last_day(add_months(AsOfMonthYear,regexp_replace(Type,'SYSF','')))")) \
                .when(pivoted_df.Type.startswith('FBGT'),expr("last_day(add_months(AsOfMonthYear,regexp_replace(Type,'FBGT','')))")) \
                .when(pivoted_df.Type.startswith('FBGD'),expr("last_day(add_months(AsOfMonthYear,regexp_replace(Type,'FBGD','')))")) \
                .when(pivoted_df.Type.startswith('AD3F'),expr("last_day(add_months(AsOfMonthYear,regexp_replace(Type,'AD3F','')))")) \
                .when(pivoted_df.Type.startswith('UAF3'),expr("last_day(add_months(AsOfMonthYear,regexp_replace(Type,'UAF3','')))")) \
                .otherwise(pivoted_df.Type).cast("date")) \
.withColumn("generationDate",last_day(add_months(col("AsOfMonthYear"),1)))

# pivoted_df = pivoted_df.filter("ASOFYR=2023 and ASOFMN=2")
# pivoted_df.display()
# pivoted_df.createOrReplaceTempView('vwDOForecast')

# creating separate datasets for RSLF, SYSF and others
RSLFdf=pivoted_df.alias("RSLF").withColumnRenamed("value","resultantNetForecast").filter(pivoted_df.Type.startswith('RSLF'))
MFCIdf=pivoted_df.alias("MFCI").withColumnRenamed("value","MFCIForecast").filter(pivoted_df.Type.startswith('MFCI'))
SYSFdf=pivoted_df.alias("SYSF").withColumnRenamed("value","unForceStatisticalForecast").filter(pivoted_df.Type.startswith('SYSF'))
AD1Fdf=pivoted_df.alias("AD1F").withColumnRenamed("value","ADS1Forecast").filter(pivoted_df.Type.startswith('AD1F'))
FBGTdf=pivoted_df.alias("FBGT").withColumnRenamed("value","futureBudgetUnits").filter(pivoted_df.Type.startswith('FBGT'))
FBGDdf=pivoted_df.alias("FBGD").withColumnRenamed("value","futureBudgetValue").filter(pivoted_df.Type.startswith('FBGD'))
AD3Fdf=pivoted_df.alias("AD3F").withColumnRenamed("value","AD3FForecast").filter(pivoted_df.Type.startswith('AD3F'))
UAF3df=pivoted_df.alias("UAF3").withColumnRenamed("value","UAF3Forecast").filter(pivoted_df.Type.startswith('UAF3'))

unpivoted_df = logility_df.select(unpivot_cols)
unpivoted_df = rename_columns(unpivoted_df, dict_renamecol)

# Join the datasets to build the final dataset
main = unpivoted_df.alias("unpivoted").join(RSLFdf,col('unpivoted.JoinID') == col('RSLF.JoinID'),"left") \
.join(SYSFdf,(col('RSLF.JoinID') == col('SYSF.JoinID')) & (col('RSLF.forecastMonth') ==  col('SYSF.forecastMonth')),"left") \
.join(MFCIdf,(col('RSLF.JoinID') == col('MFCI.JoinID')) & (col('RSLF.forecastMonth') ==  col('MFCI.forecastMonth')),"left") \
.join(AD1Fdf,(col('RSLF.JoinID') == col('AD1F.JoinID')) & (col('RSLF.forecastMonth') ==  col('AD1F.forecastMonth')),"left") \
.join(FBGTdf,(col('RSLF.JoinID') == col('FBGT.JoinID')) & (col('RSLF.forecastMonth') ==  col('FBGT.forecastMonth')),"left") \
.join(FBGDdf,(col('RSLF.JoinID') == col('FBGD.JoinID')) & (col('RSLF.forecastMonth') ==  col('FBGD.forecastMonth')),"left") \
.join(AD3Fdf,(col('RSLF.JoinID') == col('AD3F.JoinID')) & (col('RSLF.forecastMonth') ==  col('AD3F.forecastMonth')),"left") \
.join(UAF3df,(col('RSLF.JoinID') == col('UAF3.JoinID')) & (col('RSLF.forecastMonth') ==  col('UAF3.forecastMonth')),"left") \
.withColumn("_SOURCE",lit(source)) \
.withColumn("_SOURCEFILENAME",col("_SOURCEFILENAME")) \
.withColumn("_ID",sha2(concat_ws("||","forecastLevel","forecast1Id","forecast2Id","forecast3Id","RSLF.generationDate","RSLF.forecastMonth","_SOURCE"),256)) \
.withColumn("unForceStatisticalForecast",col("unForceStatisticalForecast").cast("numeric")) \
.withColumn("resultantNetForecast",col("resultantNetForecast").cast("numeric")) \
.withColumn("ADS1Forecast",col("ADS1Forecast").cast("numeric")) \
.withColumn("futureBudgetUnits",col("futureBudgetUnits").cast("numeric")) \
.withColumn("futureBudgetValue",col("futureBudgetValue").cast("numeric")) \
.withColumn("AD3FForecast",col("AD3FForecast").cast("numeric")) \
.withColumn("UAF3Forecast",col("UAF3Forecast").cast("numeric")) \
.withColumn("monthsLag",months_between(col("RSLF.forecastMonth"),col("RSLF.generationDate"))) \
.withColumn("createdOn",lit(currenttime)) \
.withColumn("modifiedOn",lit(currenttime)) \
.withColumn("createdBy",lit(currentuser)) \
.withColumn("modifiedBy",lit(currentuser)) \
.select(FinalColList)

# main.display()
main.createOrReplaceTempView('vwDOForecast')

# COMMAND ----------

# VALIDATE DATA
valid_count_rows(main, '_ID')

# COMMAND ----------

if incremental:
  register_hive_table(main, target_table, target_folder, options = {'overwrite': overwrite, 'partition_column':'forecastMonth'})
  merge_into_table(main, target_table, key_columns, options = {'auto_merge': True})  
else:  
  register_hive_table(main, target_table, target_folder, options = {'overwrite': overwrite, 'partition_column':'forecastMonth'})
  append_into_table(main, target_table,{"incremental_column":"generationDate"})

# COMMAND ----------

# DBTITLE 1,Update MonthsLag column to 0 when max generationdate
spark.sql(f"UPDATE {target_table} set monthsLag = 0 where generationDate = (select max(generationDate) from {target_table})")

# COMMAND ----------

# UPDATE CUTOFF VALUE
cutoff_value = get_max_value(main, 'modifiedOn')
update_cutoff_value(cutoff_value, target_table, source_table)
update_run_datetime(run_datetime, target_table, source_table)
