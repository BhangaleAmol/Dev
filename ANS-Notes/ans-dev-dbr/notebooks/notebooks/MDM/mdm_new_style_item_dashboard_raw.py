# Databricks notebook source
dbutils.widgets.text("source_folder", "","") 
sourceFolder = getArgument("source_folder")
sourceFolder = '/l1_raw_data' if sourceFolder == '' else sourceFolder

dbutils.widgets.text("target_folder", "","") 
targetFolder = getArgument("target_folder")
targetFolder = '/l2_clean_data' if targetFolder == '' else targetFolder

# COMMAND ----------

# MAGIC %run ./SHARED/functions

# COMMAND ----------

fileSystemUrl = get_filesystem_url()
sourceFolderUrl = fileSystemUrl + sourceFolder + '/'
targetFolderUrl = fileSystemUrl + targetFolder + '/'

# COMMAND ----------

sourceFileUrl = targetFolderUrl + 'all_change_orders.csv'
change_order_details_df = spark.read.load(sourceFileUrl, format="csv", delimiter="|", inferSchema="false", header="true", quote="\"", escape="\"", multiline="true")

sourceFileUrl = sourceFolderUrl + 'PDH_All_Change_Orders_Affected_Items.csv'
change_orders_affected_items_df1 = spark.read.load(sourceFileUrl, format="csv", inferSchema="true", header="true", quote="\"", escape="\"", multiline="true")

# sourceFileUrl = sourceFolderUrl + 'PDHSTG.OUTB_STG_PDH_DASHBOARD.par'
# number_of_sku_by_style_item_df = spark.read.load(sourceFileUrl)

number_of_sku_by_style_item_df = spark.table("mdm.pdhstg_OUTB_STG_PDH_DASHBOARD_19nov20")

# COMMAND ----------

sourceFileUrl = sourceFolderUrl + 'PDH_Fast_Download_All_Items_AMER.csv'
fast_download_all_items_amer_df = spark.read.load(sourceFileUrl, format="csv", inferSchema="true", header="true", quote="\"", escape="\"")
fast_download_all_items_amer_df.createOrReplaceTempView("fast_download_all_items_amer")

sourceFileUrl = sourceFolderUrl + 'PDH_Fast_Download_All_Items_APAC.csv'
fast_download_all_items_apac_df = spark.read.load(sourceFileUrl, format="csv", inferSchema="true", header="true", quote="\"", escape="\"")
fast_download_all_items_apac_df.createOrReplaceTempView("fast_download_all_items_apac")

sourceFileUrl = sourceFolderUrl + 'PDH_Fast_Download_All_Items_EMEA.csv'
fast_download_all_items_emea_df = spark.read.load(sourceFileUrl, format="csv", inferSchema="true", header="true", quote="\"", escape="\"")
fast_download_all_items_emea_df.createOrReplaceTempView("fast_download_all_items_emea")

sourceFileUrl = sourceFolderUrl + 'PDH_Fast_Download_All_Items_GLOBAL_SKU.csv'
fast_download_all_items_global_sku_df = spark.read.load(sourceFileUrl, format="csv", inferSchema="true", header="true", quote="\"", escape="\"")
fast_download_all_items_global_sku_df.createOrReplaceTempView("fast_download_all_items_global_sku_df")

sourceFileUrl = sourceFolderUrl + 'PDH_Fast_Download_All_Items_GLOBAL_STYLE.csv'
fast_download_all_items_global_style_df = spark.read.load(sourceFileUrl, format="csv", inferSchema="true", header="true", quote="\"", escape="\"")
fast_download_all_items_global_style_df.createOrReplaceTempView("fast_download_all_items_global_style_df")

# COMMAND ----------

display(fast_download_all_items_global_sku_df)

# COMMAND ----------

fast_download_all_items_df = spark.sql("""
  SELECT * FROM fast_download_all_items_amer
  UNION
  SELECT * FROM fast_download_all_items_apac
  UNION
  SELECT * FROM fast_download_all_items_emea
  UNION
  SELECT * FROM fast_download_all_items_global_sku_df
  UNION
  SELECT * FROM fast_download_all_items_global_style_df
""")

display(fast_download_all_items_df)

# COMMAND ----------

#EP: Correct
print(change_order_details_df.count())
print(change_orders_affected_items_df1.count())
print(fast_download_all_items_df.count())
print(number_of_sku_by_style_item_df.count())

# COMMAND ----------

# transformation functions
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as f
import numpy as np
from datetime import date
import datetime

def transform(self, f):
  return f(self)
DataFrame.transform = transform

def add_customs_complete(df):
  return df.withColumn("CUSTOMS_COMPLETE", 
    f.when(f.col("PPM") == "BULK LOAD", f.col("ITEM_CREATION_DATE"))
    .when((f.col("PPM") != "BULK LOAD") & (f.col("CHANGE_ORDER_CREATION_DATE").isNull()) & (f.col("ITEM_STATUS") != "New"), f.col("ITEM_CREATION_DATE"))
    .when((f.col("PPM") != "BULK LOAD") & (f.col("CHANGE_ORDER_CREATION_DATE").isNotNull()), f.col("CHANGE_ORDER_IMPLEMENTATION_DATE"))
    .otherwise(None))

def add_days_customs(df):
  days_customs_udf = udf(lambda x, y, days_rmdm: 0 if (x is None) | (y is None) else max(0, int(np.busday_count(x, y)) - days_rmdm), IntegerType())
  return df.withColumn("DAYS_CUSTOMS",
    f.when(f.col("CUSTOMS_COMPLETE").isNull(), 0)
    .otherwise(
      days_customs_udf(
        f.date_format(f.col("ITEM_CREATION_DATE"), "yyyy-MM-dd"), 
        f.date_format(f.col("CUSTOMS_COMPLETE"), "yyyy-MM-dd"),
        f.col("DAYS_RMDM"))))

def add_days_rmdm(df):
  days_rmdm_udf = udf(lambda x, y: 0 if (x is None) | (y is None) else max(0, int(np.busday_count(x, y))), IntegerType())
  return df.withColumn("DAYS_RMDM",
    f.when(f.col("RMDM_COMPLETE").isNull(), 0)
    .otherwise(
      days_rmdm_udf(
        f.date_format(f.col("ITEM_CREATION_DATE"), "yyyy-MM-dd"), 
        f.date_format(f.col("RMDM_COMPLETE"), "yyyy-MM-dd")))) 

def add_days_total(df):
  return df.withColumn("DAYS_TOTAL", f.col("DAYS_RMDM") + f.col("DAYS_CUSTOMS"))

def add_days_total_till_today(df):
  def days_total_till_today(x):
    if (x is None):
      return 0
    else:
      today = date.today() + datetime.timedelta(days=1)
      return int(np.busday_count(x, today.strftime("%Y-%m-%d")))
    
  days_total_till_today_udf = f.udf(days_total_till_today, IntegerType())
  return df.withColumn("DAYS_TOTAL_TILL_TODAY", 
    f.when(f.col("CUSTOMS_COMPLETE").isNotNull(), f.col("DAYS_TOTAL"))
    .otherwise(
      days_total_till_today_udf(
        f.date_format(f.col("ITEM_CREATION_DATE"), "yyyy-MM-dd")
      )))

def add_itemorg(df):
  return df.withColumn("ITEMORG", f.concat(f.col("ITEM_NAME"), f.col("REGION")))

def add_itemorg_affected_items(df):
  substring_udf = udf(lambda x:x[0:5],StringType())
  return df.withColumn("ITEMORG", f.concat(f.col("ITEM_NAME"), f.trim(substring_udf(f.col("CHANGE_ORDER_TYPE_NAME")))))

def add_month_started(df):
  return df.withColumn("MONTH_STARTED", f.date_format(f.col("ITEM_CREATION_DATE"), 'yyyy-MM'))

def add_month_completed(df):
  return df.withColumn("MONTH_COMPLETED", 
    f.when(f.col("CUSTOMS_COMPLETE").isNull(), None)
    .otherwise(f.date_format(f.col("CUSTOMS_COMPLETE"), 'yyyy-MM')))

def add_number_of_sku_linked_to_style_item(df):
  return df.withColumn("NUMBER_OF_SKU_LINKED_TO_STYLE_ITEM",
    f.when(f.col("COUNT_NUMBER_OF_SKU_REGION").isNull(), None)
     .otherwise(f.col("COUNT_NUMBER_OF_SKU_REGION")))

def add_ppm_column(df):
  return df.withColumn("PPM", 
    f.when(f.col("ITEM_CREATED_BY") == "shobhit.sahu@ansell.com", "BULK LOAD")
    .when(f.col("ITEM_CREATED_BY") == "venu.narapureddy@ansell.com", "BULK LOAD")
    .when(f.col("ITEM_CREATED_BY") == "bipin.rajalingam@ansell.com", "BULK LOAD")
    .otherwise(f.col("ITEM_CREATED_BY")))

def add_rmdm_complete(df):
  return df.withColumn("RMDM_COMPLETE", 
    f.when(f.col("PPM") == "BULK LOAD", f.col("ITEM_CREATION_DATE"))
    .when((f.col("PPM") != "BULK LOAD") & (f.col("CHANGE_ORDER_CREATION_DATE").isNull()) & (f.col("ITEM_STATUS") != "New"), f.col("ITEM_CREATION_DATE"))
    .when((f.col("PPM") != "BULK LOAD") & (f.col("CHANGE_ORDER_CREATION_DATE").isNotNull()), f.col("CHANGE_ORDER_CREATION_DATE"))
    .otherwise(None))

def apply_column_function(columns, func):
  def inner(df):
    for c_name in columns:
      df = df.withColumn(c_name, 
        f.when(f.col(c_name).isNotNull(), func(f.col(c_name)))
        .otherwise(None))
    return df
  return inner

def filter_change_order_internal_status(df):
  return df.filter(f.col("CHANGE_ORDER_INTERNAL_STATUS") != "Canceled") 

def filter_item_style_item(df):
  return df.filter(f.col("ITEM_STYLE_ITEM") == "Y")

def filter_nulls(columns):
  def inner(df):
    return df.na.drop(subset=[*columns])
  return inner

def group_by_item_name_sbu_region(df):
  return df.groupBy("ITEM_NAME", "SBU", "REGION").agg(f.count("GPID").alias('COUNT_NUMBER_OF_SKU_REGION'))

def rename_column(column, name):
  def inner(df):
     return df.withColumnRenamed(column, name)
  return inner

def replace_nulls_with_unknown(columns):
  def inner(df):
    return df.na.fill("Unknown", columns)
  return inner

# COMMAND ----------

# MAGIC %md
# MAGIC change_order_details_df

# COMMAND ----------

display(change_order_details_df)

# COMMAND ----------

change_order_details_cl_df = (
  change_order_details_df
  .transform(filter_change_order_internal_status)
)

change_order_details_cl_df.createOrReplaceTempView("change_order_details")
display(change_order_details_cl_df)

# COMMAND ----------

# MAGIC %md
# MAGIC fast_download_all_items_df

# COMMAND ----------

display(fast_download_all_items_df)

# COMMAND ----------

fast_download_all_items_cl_df = (
  fast_download_all_items_df
  .transform(filter_item_style_item)
  .transform(rename_column('ORGANIZATION_NAME', 'REGION'))
  .transform(add_itemorg)
  .transform(filter_nulls(["ITEMORG"]))
  .transform(apply_column_function(['ITEMORG'], f.upper))
  .transform(apply_column_function(['ITEM_CREATED_BY'], f.lower))
  .transform(apply_column_function(['ITEM_LAST_UPDATED_BY'], f.lower))  
  .transform(parse_timestamp([
    'ITEM_CREATION_DATE',
    'ITEM_LAST_UPDATED_DATE'
  ]))
)

fast_download_all_items_cl_df.createOrReplaceTempView("fast_download_all_items")
display(fast_download_all_items_cl_df)

# COMMAND ----------

# MAGIC %md
# MAGIC change_orders_affected_items

# COMMAND ----------

display(change_orders_affected_items_df1)

# COMMAND ----------

change_orders_affected_items_df2 = (
  change_orders_affected_items_df1
  .transform(rename_column('COLUMN15', 'CHANGE_ORDER_INTERNAL_STATUS'))
  .transform(rename_column('AFFECTED_ITEM_NAME', 'ITEM_NAME'))
  .transform(filter_change_order_internal_status)
  .transform(apply_column_function(['CHANGE_ORDER_CREATED_BY'], f.lower))
  .transform(apply_column_function([
    'CHANGE_ORDER_NUMBER', 
    'CHANGE_ORDER_NAME'], f.upper))
  .transform(add_itemorg_affected_items)
  .transform(filter_nulls(["ITEMORG"]))
  .transform(apply_column_function(['ITEMORG'], f.upper))
  .transform(replace_nulls_with_unknown(["CHANGE_ORDER_NUMBER"]))
  .transform(parse_month)
  .transform(parse_timestamp([
    'CHANGE_ORDER_CREATION_DATE',
    'CHANGE_ORDER_INITIATION_DATE',
    'CHANGE_ORDER_APPROVAL_DATE',
    'EFFECTIVE_DATE',
    'CHANGE_ORDER_IMPLEMENTATION_DATE'
  ]))
)

change_orders_affected_items_df2.createOrReplaceTempView("change_orders_affected_items_2")
display(change_orders_affected_items_df2)

# COMMAND ----------

# Keep only the oldest changes
change_orders_affected_items_df3 = spark.sql("""
  SELECT * FROM (
    SELECT 
      ai.*, 
      row_number() OVER (PARTITION BY ai.ITEMORG ORDER BY ai.CHANGE_ORDER_CREATION_DATE ASC) AS _NUM
    FROM change_orders_affected_items_2 ai
  )
  WHERE _NUM = 1
""")

change_orders_affected_items_df3.createOrReplaceTempView("change_orders_affected_items")
display(change_orders_affected_items_df3)

# COMMAND ----------

# MAGIC %md
# MAGIC number_of_sku_by_style_item_df

# COMMAND ----------

display(number_of_sku_by_style_item_df)

# COMMAND ----------

number_of_sku_by_style_item_cl_df = (
  number_of_sku_by_style_item_df
  .transform(rename_column('STYLE_ITEM', 'ITEM_NAME'))
  .transform(group_by_item_name_sbu_region)
  .transform(add_itemorg)
  .transform(filter_nulls(["ITEMORG"]))
  .transform(apply_column_function(['ITEMORG'], f.upper))
)

number_of_sku_by_style_item_cl_df.createOrReplaceTempView("number_of_sku_by_style_item")
display(number_of_sku_by_style_item_cl_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Merge Data

# COMMAND ----------

merge_df = spark.sql("""
  SELECT fd.*, cod.*, sku.SBU, sku.COUNT_NUMBER_OF_SKU_REGION
  FROM fast_download_all_items fd
  LEFT JOIN change_orders_affected_items co ON fd.ITEMORG = co.ITEMORG
  LEFT JOIN change_order_details cod ON co.CHANGE_ORDER_NUMBER = cod.CHANGE_ORDER_NUMBER
  LEFT JOIN number_of_sku_by_style_item sku ON fd.ITEMORG = sku.ITEMORG
""")

display(merge_df)

# COMMAND ----------

print(merge_df.count())

# COMMAND ----------

merge_cl_df = (
  merge_df
  .transform(add_ppm_column)
  .transform(add_customs_complete)
  .transform(add_month_started)
  .transform(add_month_completed)
  .transform(add_rmdm_complete)
  .transform(add_days_rmdm)
  .transform(add_days_customs)
  .transform(add_days_total)
  .transform(add_days_total_till_today)
  .transform(add_number_of_sku_linked_to_style_item)
)

merge_cl_df.createOrReplaceTempView("merge_cl_df")
print(merge_cl_df.count())

# COMMAND ----------

# MAGIC %md Save Data

# COMMAND ----------

destFolderUrl = targetFolderUrl + 'new_style_item_dashboard'
merge_cl_df.repartition(1).write.save(destFolderUrl, format="csv", sep="|", header="true", mode="overwrite")
fileName = list(filter(lambda r : r.name.endswith(".csv"), dbutils.fs.ls(destFolderUrl)))[0].name

destFileUrl = targetFolderUrl + 'new_style_item_dashboard/' + fileName
destSingleFileUrl = targetFolderUrl + 'new_style_item_dashboard.csv'
dbutils.fs.cp(destFileUrl, destSingleFileUrl)

# COMMAND ----------

# read saved file
# test_df = spark.read.load(destSingleFileUrl, format="csv", inferSchema="false", header="true", delimiter="|")
# test_df.createOrReplaceTempView("test_df")
# display(test_df)

# COMMAND ----------


