# Databricks notebook source
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import BooleanType, StringType, TimestampType
import pyspark.sql.functions as f
from pytz import timezone

def transform(self, f):
  return f(self)
DataFrame.transform = transform

def attach_unknown_record(options = {}):
  def inner(df):
    record_df = get_unknown_record(df.schema, options)
    return record_df.union(df)
  return inner

def drop_duplicates(columns):
  def inner(df):
    return df.dropDuplicates(subset = columns)
  return inner

def explode_location(df):
  df = df.select("*", 
    f.split("Location", ">").getItem(0).alias("Region"),
    f.split("Location", ">").getItem(1).alias("Country"),
    f.split("Location", ">").getItem(2).alias("City")
  ).drop("Location")

  for c_name in ["Region", "Country", "City"]:
    df = df.withColumn(c_name, f.trim(f.col(c_name)))
  return df

def get_unknown_record(schema, options = {}):
  
  default_value = options.get('default_value', 'Unknown')
  key_columns = options.get('key_columns', [])
  
  row = spark.range(1).drop("id")
  for column in schema:
    if column.name == "_SOURCE":
      row = row.withColumn("_SOURCE", f.lit("DEFAULT"))      
    elif column.name == "_DELETED":
      row = row.withColumn("_DELETED", f.lit(False))
    elif column.name == "_MODIFIED":
      row = row.withColumn("_MODIFIED", f.current_timestamp())  
    elif column.name in key_columns:
      row = row.withColumn(column.name, f.lit(0))    
    elif str(column.dataType) == 'StringType':
      row = row.withColumn(column.name, f.lit(default_value))
    else:
      row = row.withColumn(column.name, f.lit(None))      
  return row

def localize_cst6cdt(df):
  func = f.udf(lambda x: None if x == None else timezone('CST6CDT').localize(x), TimestampType())
  df = df.withColumn('CreatedDate', f.when(f.col('mailgate') == 0, func(df.CreatedDate)).otherwise(df.CreatedDate))
  df = df.withColumn('DueDate', f.when((f.col('mailgate') == 0) | ((f.col('mailgate') == 1) & (f.col('sla_change') == 1)), func(df.DueDate)).otherwise(df.DueDate))
  df = df.withColumn('TimeToOwnDate', f.when(f.col('mailgate') == 0, func(df.TimeToOwnDate)).otherwise(df.TimeToOwnDate))
  df = df.withColumn('AssignmentDate', func(df.AssignmentDate))
  df = df.withColumn('SolveDate', func(df.SolveDate))
  df = df.withColumn('CloseDate', func(df.CloseDate))
  df = df.withColumn('ModifiedDate', f.when(f.col('mailgate') == 0, func(df.ModifiedDate)).otherwise(df.ModifiedDate))
  return df

def lower_column(column):
  def inner(df):
    return df.withColumn(column, f.lower(f.col(column)))
  return inner

def rename_city(df):
  df = df.withColumn('City', f.when(df['Country'] == 'Remote - AMER', 'Remote - AMER').otherwise(df['City'])) 
  df = df.withColumn('City', f.when(df['Country'] == 'Remote - APAC', 'Remote - APAC').otherwise(df['City'])) 
  df = df.withColumn('City', f.when(df['Country'] == 'Remote - EMEA', 'Remote - EMEA').otherwise(df['City'])) 
  df = df.replace(['Kedah Site B', 'KHT', 'HGL', 'ShahAlam', 'Shanghai Feidun'], ['Kedah', 'Kulim', 'Shah Alam', 'Shah Alam', 'Shanghai'], 'City')
  return df

def rename_country(df):
  return df.replace('Remote - AM', 'Remote - AMER', 'Country')

def rename_region(df):
  return df.replace('Americas', 'AMER', 'Region')

def replace_empty_with_unknown(columns):
  def inner(df):
    for c_name in columns:
      df = df.withColumn(c_name, f.when(f.col(c_name) == '', 'Unknown').otherwise(f.col(c_name)))
    return df
  return inner

def replace_nulls_with_unknown(columns):
  def inner(df):
    return df.na.fill("Unknown", columns)
  return inner

def replace_parent_entity_value(df):
  return df.withColumn("ParentEntityNK", f.when(f.col("ParentEntityNK") == -1, None).otherwise(f.col("ParentEntityNK")))

def translate_impact(df):
  return df.withColumn("impact", f.when(f.col("impact") == 1, "High")
   .when(f.col("impact") == 2, "Low")
   .when(f.col("impact") == 3, "Medium")
   .otherwise("Unknown"))

def translate_priority(df):
  return df.withColumn("priority", f.when(f.col("priority") == 1, "Very Low")
   .when(f.col("priority") == 2, "Low")
   .when(f.col("priority") == 3, "Medium")
   .when(f.col("priority") == 4, "High")
   .when(f.col("priority") == 5, "Very High")
   .otherwise("Unknown"))

def translate_status(df):
  return df.withColumn("status", 
  f.when(f.col("status") == 1, "New")
   .when(f.col("status") == 2, "Processing (assigned)")
   .when(f.col("status") == 3, "Processing (planned)")
   .when(f.col("status") == 4, "Pending")
   .when(f.col("status") == 5, "Solved")
   .when(f.col("status") == 6, "Closed")
   .when(f.col("status") == 7, "Accepted")
   .when(f.col("status") == 8, "Review")
   .when(f.col("status") == 9, "Evaluation")
   .when(f.col("status") == 10, "Approval")
   .when(f.col("status") == 11, "Testing")
   .when(f.col("status") == 12, "Qualification")
   .when(f.col("status") == 13, "Refused")
   .when(f.col("status") == 14, "Cancelled")
   .otherwise("Unknown"))

def translate_type(df):
  return df.withColumn("type", f.when(f.col("type") == 1, "Incident")
   .when(f.col("type") == 2, "Request")
   .when(f.col("type") == 3, "Project")
   .otherwise("Unknown"))

def translate_urgency(df):
  return df.withColumn("urgency", f.when(f.col("urgency") == 1, "Very Low")
   .when(f.col("urgency") == 2, "Low")
   .when(f.col("urgency") == 3, "Medium")
   .when(f.col("urgency") == 4, "High")
   .when(f.col("urgency") == 5, "High")
   .otherwise("Unknown"))

def trim_column(columns):
  def inner(df):
    for c_name in columns:
      df = df.withColumn(c_name, f.trim(f.col(c_name)))
    return df
  return inner

# COMMAND ----------


