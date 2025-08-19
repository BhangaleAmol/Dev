# Databricks notebook source
import os

def get_filesystem_url():
  envName = os.getenv('ENV_NAME')
  secretScope = 'edm-ans-{}-dbr-scope'.format(envName)
  configOption = 'fs.azure.account.key.edmans{}data001.dfs.core.windows.net'.format(envName)
  accessKey = dbutils.secrets.get(scope = secretScope, key = "ls-azu-datalake-key")
  spark.conf.set(configOption, accessKey)
  return "abfss://mdm@edmans{}data001.dfs.core.windows.net".format(envName)

# COMMAND ----------

from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as f
import numpy as np
import datetime
from datetime import date

# COMMAND ----------

def transform(self, f):
  return f(self)
DataFrame.transform = transform

def apply_column_function(columns, func):
  def inner(df):
    for c_name in columns:
      df = df.withColumn(c_name, 
        f.when(f.col(c_name).isNotNull(), func(f.col(c_name)))
        .otherwise(None))
    return df
  return inner

def parse_date_column(columns):
  def inner(df):
    date_formats = ["dd-MM-yyyy", "MM/dd/yyyy", "M/d/yyy"]
    for c_name in columns:
      df = df.withColumn(c_name, 
        f.when(f.col(c_name) == "2099-01-25T00:00:00", None)
        .otherwise(f.coalesce(*[f.to_date(f.col(c_name), fmt) for fmt in date_formats])))
    return df
  return inner

def parse_month(df):
  return df.withColumn("MONTH", f.to_date(f.col("MONTH"), "yyyy / MM"))

def parse_timestamp(columns):
  def inner(df):
    for c_name in columns:
      df = df.withColumn(c_name, f.from_unixtime(f.unix_timestamp(c_name, "yyyy-MM-dd'T'HH:mm:ss.SSS"), 'yyyy-MM-dd HH:mm:ss'))
    return df
  return inner

def rename_column(column, name):
  def inner(df):
     return df.withColumnRenamed(column, name)
  return inner

# COMMAND ----------

# UDF Functions

def days_between(x, y):
  global days_between #added gdp 20201214
  return 0 if (x is None) | (y is None) else int(np.busday_count(x, y))
days_between_udf = f.udf(days_between, IntegerType())

#22.04.2020 EP
#def days_till_today(x):
#  today = date.today() + datetime.timedelta(days=1)
#  return int(np.busday_count(x, today.strftime("%Y-%m-%d")))
#days_till_today_udf = f.udf(days_till_today, IntegerType())


def days_till_today(x):
  today = date.today() + datetime.timedelta(days=1)
  return 0 if x is None else int(np.busday_count(x, today.strftime("%Y-%m-%d")))
days_till_today_udf = f.udf(days_till_today, IntegerType())
