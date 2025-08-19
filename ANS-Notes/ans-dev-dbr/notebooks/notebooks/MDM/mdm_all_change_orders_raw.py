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
sourceFileUrl = fileSystemUrl + sourceFolder + '/PDH_All_Change_Orders.csv'
targetFolderUrl = fileSystemUrl + targetFolder + '/'

# COMMAND ----------

all_change_orders_raw_df = spark.read.load(sourceFileUrl, format="csv", inferSchema="true", header="true", quote="\"", escape="\"", multiLine="true")
all_change_orders_raw_df.createOrReplaceTempView("all_change_orders_raw")

# COMMAND ----------

# from pyspark.sql.types import IntegerType, StringType, DateType
# from pyspark.sql.dataframe import DataFrame
# import pyspark.sql.functions as f
# import re

# def transform(self, f):
#   return f(self)
# DataFrame.transform = transform

# def parse_month(df):
#   return df.withColumn("MONTH", f.to_date(f.col("MONTH"), "yyyy / MM"))

# def apply_column_function(columns, func):
#   def inner(df):
#     for c_name in columns:
#       df = df.withColumn(c_name, 
#         f.when(f.col(c_name).isNotNull(), func(f.col(c_name)))
#         .otherwise(None))
#     return df
#   return inner

# def parse_timestamp(columns):
#   def inner(df):
#     for c_name in columns:
#       df = df.withColumn(c_name, f.from_unixtime(f.unix_timestamp(c_name, 'yyyy-MM-dd HH:mm:ss.SSS'), 'yyyy-MM-dd HH:mm:ss'))
#     return df
#   return inner

# COMMAND ----------

all_change_orders_cl_df = (
  all_change_orders_raw_df
    .transform(parse_month)
    .transform(apply_column_function(['CHANGE_ORDER_CREATED_BY'], f.lower))
    .transform(apply_column_function([
      'CHANGE_ORDER_NUMBER', 
      'CHANGE_ORDER_NAME'], f.upper))
  .transform(parse_timestamp([
    'CHANGE_ORDER_CREATION_DATE',
    'CHANGE_ORDER_INITIATION_DATE',
    'CHANGE_ORDER_APPROVAL_DATE',
    'CHANGE_ORDER_CANCELLATION_DATE',
    'CHANGE_ORDER_IMPLEMENTATION_DATE'
  ]))
    
)

all_change_orders_cl_df.createOrReplaceTempView("all_change_orders")
display(all_change_orders_cl_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Save File

# COMMAND ----------

destFolderUrl = targetFolderUrl + 'all_change_orders'
all_change_orders_cl_df.repartition(1).write.save(destFolderUrl, format="csv", sep="|", header="true", mode="overwrite", multiline=True)
fileName = list(filter(lambda r : r.name.endswith(".csv"), dbutils.fs.ls(destFolderUrl)))[0].name
destFileUrl = targetFolderUrl + 'all_change_orders/' + fileName
destSingleFileUrl = targetFolderUrl + 'all_change_orders.csv'
dbutils.fs.cp(destFileUrl, destSingleFileUrl)

# COMMAND ----------

# MAGIC %md
# MAGIC Test Data

# COMMAND ----------

test_df = spark.read.load(destSingleFileUrl, format="csv", inferSchema="false", header="true", delimiter="|", multiline=True)
test_df.createOrReplaceTempView("test_df")

print('IN: ' + str(all_change_orders_raw_df.count()))
print('OUT: ' + str(test_df.count()))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM test_df
# MAGIC WHERE CHANGE_ORDER_TYPE_NAME = 'EMEA Styles Change Order' AND CHANGE_ORDER_IMPLEMENTATION_DATE IS NULL AND CHANGE_ORDER_STATUS_TYPE = 8
# MAGIC ORDER BY CHANGE_ORDER_NUMBER

# COMMAND ----------


