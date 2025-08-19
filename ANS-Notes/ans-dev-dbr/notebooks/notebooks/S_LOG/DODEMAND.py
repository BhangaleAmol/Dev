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
table_name = get_input_param('table_name', default_value = 'dodemand')
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
source_table = 'logftp.logility_do_actuals_export_for_dea_refresh'

if table_name.startswith('LVL3_'):
  source_table = 'logftp.logility_do_lvl3_actuals_export_for_dea_refresh'
  viewName = 'logility_do_lvl3_actuals_export_for_dea_refresh'
else:
  source_table = 'logftp.logility_do_actuals_export_for_dea_refresh'
  viewName = 'logility_do_actuals_export_for_dea_refresh'

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(target_table, source_table, prune_days)
  print(cutoff_value)
  logility_do_actuals_export_for_dea_refresh = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  logility_do_actuals_export_for_dea_refresh = load_full_dataset(source_table)

logility_do_actuals_export_for_dea_refresh.createOrReplaceTempView(viewName)

# COMMAND ----------

# checking if the file is already loaded then dont load it again and set back the max to the previous value
if incremental:
  rowscount=logility_do_actuals_export_for_dea_refresh.count()
  sql=f"select if(count(*)>0, true, false) as isFileLoaded from {source_table} where _SOURCEFILENAME IN (select distinct _SOURCEFILENAME from {viewName})"
  df = spark.sql(sql)
  isFileLoaded = df.collect()[0][0]
  print(rowscount,isFileLoaded)
  
  if isFileLoaded or rowscount==0:
    dbutils.notebook.exit(json.dumps({"rowscount": rowscount}))

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import *

def rename_columns(df, columns):
    if isinstance(columns, dict):
        return df.select(*[col(col_name).alias(columns.get(col_name, col_name)) for col_name in df.columns])
    else:
        raise ValueError("'columns' should be a dict, like {'old_name_1':'new_name_1', 'old_name_2':'new_name_2'}")

# COMMAND ----------

# DBTITLE 1,Demand
IDColumns = ["FCSTLVL","Lvl1Fcst","Lvl2Fcst","Lvl3Fcst","ASOFMN","ASOFYR"]  
key_column = "JoinID"
pivotColumns = ("FUTO","ADSD","ADJD","ACTD","DMDCUR")
excludeColumns = ["ENTIREKEY","ADRLTHLD1","ADRLTHLD2","FUTSYSHLD1","FUTSYSHLD2","Field-47"]
FinalColList = ["_ID","forecastLevel","forecast1Id","forecast2Id","forecast3Id","uom","actualDemand","futureDemand","ADSDShipments","adjustedDemand","generationDate","forecastMonth","_SOURCEFILENAME","createdOn","modifiedOn","createdBy","modifiedBy"]
dict_renamecol = {'FCSTLVL':'forecastLevel', 'Lvl1Fcst':'forecast1Id', 'Lvl2Fcst':'forecast2Id', 'Lvl3Fcst':'forecast3Id'} 

logility_df=logility_do_actuals_export_for_dea_refresh.withColumn("JoinID",sha2(concat_ws("||",array(IDColumns)),256))
# .withColumnRenamed("DMDCUR","ACTD0")

pivot_cols = [x for x in logility_df.columns if x.strip().startswith(pivotColumns)]
unpivot_cols = [x for x in logility_df.columns if not x.strip().startswith(pivotColumns)]
unpivot_cols = [x for x in unpivot_cols if x not in excludeColumns] 

pivotColExpr = " ,".join([f"'{c}', {c}" for c in pivot_cols])
pivotExpr = f"stack({len(pivot_cols)}, {pivotColExpr}) as (Type,value)"
pivoted_df = logility_df \
              .select("JoinID","ASOFMN","ASOFYR",expr(pivotExpr)) \
              .filter("Type <> 'FUTO1'")

pivoted_df = pivoted_df \
.withColumn("Type",when(pivoted_df.Type == 'DMDCUR','ACTD0').otherwise(pivoted_df.Type).cast("string")) \
.withColumn("value",col("value").cast("numeric"))

# Using static period numbers in the column names
pivoted_df=pivoted_df \
    .withColumn("MonthYear",lpad(concat(col('ASOFMN'),col('ASOFYR')),6,'0')) \
    .withColumn("AsOfMonthYear",trunc(to_date(concat(substring(col('MonthYear'), 1,2),lit("-01-"),substring(col('MonthYear'), 3,4)),'MM-dd-yyyy'),"month")) \
    .withColumn("currMonth",last_day(add_months(col("AsOfMonthYear"),1))) \
    .withColumn("forecastMonth",
                when(pivoted_df.Type.startswith('ACTD'),expr("last_day(add_months(currMonth,-regexp_replace(Type,'ACTD','')))")) \
                .when(pivoted_df.Type.startswith('ADSD'),expr("last_day(add_months(currMonth,-regexp_replace(Type,'ADSD','')))")) \
                .when(pivoted_df.Type.startswith('ADJD'),expr("last_day(add_months(currMonth,-regexp_replace(Type,'ADJD','')))")) \
                .when(pivoted_df.Type.startswith('FUTO'),expr("last_day(add_months(AsOfMonthYear,regexp_replace(Type,'FUTO','')))")) \
                .otherwise(pivoted_df.Type).cast("date")) \
.withColumn("generationDate",col("currMonth"))

# Creating separate datasets for FUTO, ACTD and others
FUTOdf=pivoted_df.alias("FUTO").withColumnRenamed("value","futureDemand").filter(pivoted_df.Type.startswith('FUTO'))
ACTDdf=pivoted_df.alias("ACTD").withColumnRenamed("value","actualDemand").filter(pivoted_df.Type.startswith('ACTD'))
ADSDdf=pivoted_df.alias("ADSD").withColumnRenamed("value","ADSDShipments").filter(pivoted_df.Type.startswith('ADSD'))
ADJDdf=pivoted_df.alias("ADJD").withColumnRenamed("value","adjustedDemand").filter(pivoted_df.Type.startswith('ADJD'))

ACTDdf = ACTDdf.groupBy("JoinID","ASOFMN","ASOFYR","Type","MonthYear","AsOfMonthYear","currMonth","forecastMonth","generationDate") \
  .agg(sum("actualdemand").alias("actualdemand"))

unpivoted_df = logility_df.select(unpivot_cols)
unpivoted_df = rename_columns(unpivoted_df, dict_renamecol)

# Join all datasets to build final dataset
ACTmain = unpivoted_df.alias("unpivoted").join(ACTDdf,col('unpivoted.JoinID') == col('ACTD.JoinID'),"left") \
.join(ADSDdf,(col('ACTD.JoinID') == col('ADSD.JoinID')) & (col('ACTD.forecastMonth') ==  col('ADSD.forecastMonth')),"left") \
.join(ADJDdf,(col('ACTD.JoinID') == col('ADJD.JoinID')) & (col('ACTD.forecastMonth') ==  col('ADJD.forecastMonth')),"left") \
.withColumn("_SOURCE",lit(source)) \
.withColumn("_SOURCEFILENAME",col("_SOURCEFILENAME")) \
.withColumn("futureDemand",lit(None).cast("int").alias("futureDemand")) \
.withColumn("_ID",sha2(concat_ws("||","forecastLevel","forecast1Id","forecast2Id","forecast3Id","ACTD.generationDate","ACTD.forecastMonth","_SOURCE"),256)) \
.withColumn("createdOn",lit(currenttime)) \
.withColumn("modifiedOn",lit(currenttime)) \
.withColumn("createdBy",lit(currentuser)) \
.withColumn("modifiedBy",lit(currentuser)) \
.selectExpr("_ID","forecastLevel","forecast1Id","forecast2Id","forecast3Id","uom","actualDemand","futureDemand","ADSDShipments","adjustedDemand","ACTD.generationDate as generationDate","ACTD.forecastMonth as forecastMonth","_SOURCEFILENAME","createdOn","modifiedOn","createdBy","modifiedBy") \
.createOrReplaceTempView('ACTmain')

# Create dataset for FUTO
FUTOmain = unpivoted_df.alias("unpivoted").join(FUTOdf,col('unpivoted.JoinID') == col('FUTO.JoinID'),"left") \
.withColumn("_SOURCE",lit(source)) \
.withColumn("_SOURCEFILENAME",col("_SOURCEFILENAME")) \
.withColumn("actualDemand",lit(None).cast("int").alias("actualDemand")) \
.withColumn("ADSDShipments",lit(None).cast("int").alias("ADSDShipments")) \
.withColumn("adjustedDemand",lit(None).cast("int").alias("adjustedDemand")) \
.withColumn("_ID",sha2(concat_ws("||","forecastLevel","forecast1Id","forecast2Id","forecast3Id","generationDate","forecastMonth","_SOURCE"),256)) \
.withColumn("createdOn",lit(currenttime)) \
.withColumn("modifiedOn",lit(currenttime)) \
.withColumn("createdBy",lit(currentuser)) \
.withColumn("modifiedBy",lit(currentuser)) \
.selectExpr(FinalColList) \
.createOrReplaceTempView('FUTOmain')

# Union FUTO dataset with the Main 
main = spark.sql("""
WITH CTE (
select _ID,forecastLevel,forecast1Id,forecast2Id,forecast3Id,uom,actualDemand,futureDemand,ADSDShipments,adjustedDemand,generationDate,forecastMonth,_SOURCEFILENAME,createdOn,modifiedOn,createdBy,modifiedBy FROM ACTmain
union all
select _ID,forecastLevel,forecast1Id,forecast2Id,forecast3Id,uom,actualDemand,futureDemand,ADSDShipments,adjustedDemand,generationDate,forecastMonth,_SOURCEFILENAME,createdOn,modifiedOn,createdBy,modifiedBy FROM FUTOmain
)
select _ID,forecastLevel,forecast1Id,forecast2Id,forecast3Id,uom,generationDate,forecastMonth,sum(actualdemand) as actualdemand,sum(futureDemand) as futureDemand,sum(ADSDShipments) ADSDShipments,sum(adjustedDemand) as adjustedDemand,_SOURCEFILENAME,createdOn,modifiedOn,createdBy,modifiedBy from cte group by 
_ID,forecastLevel,forecast1Id,forecast2Id,forecast3Id,uom,generationDate,forecastMonth,_SOURCEFILENAME,createdOn,modifiedOn,createdBy,modifiedBy 
""")

# main.display()
main.createOrReplaceTempView('vwDODemand')

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

# UPDATE CUTOFF VALUE
cutoff_value = get_max_value(main, 'modifiedOn')
update_cutoff_value(cutoff_value, target_table, source_table)
update_run_datetime(run_datetime, target_table, source_table)
