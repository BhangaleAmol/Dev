# Databricks notebook source
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import *
import pyspark.sql.functions as f

import numpy as np
import re

from datetime import date, datetime, timedelta

# COMMAND ----------

# EXTEND DATAFRAME OBJECT
def transform(self, f):
  return f(self)

def transform_if(self, cond: bool ,f):
  if cond is True:
    return f(self)
  else:
    return self

DataFrame.transform = transform  
DataFrame.transform_if = transform_if  

# COMMAND ----------

# FUNCTIONS
def apply_schema(schema: dict):
  def _(df):
    for column, data_type in schema.items():
      if column in df.columns:
        df = df.withColumn(column, f.col(column).cast(data_type))

    columns = [data_type[0] for data_type in df.dtypes if data_type[1] == 'null']    
    for column in columns:
      df = df.withColumn(column, f.col(column).cast("string"))
    return df
  return _

def attach_archive_column(name = "_ARCHIVE", archive_type = "m"):
  archive_type = archive_type.lower() 
  def _(df):
    if archive_type == 'm':
      return (
        df
        .withColumn(name, f.date_trunc('month', f.current_date()).cast('date'))
      )
    
    if archive_type == 'd':
      return (
        df
        .withColumn(name, f.current_date())
      )
  return _

def attach_deleted_flag(value = False, name = "_DELETED"):
  def _(df):
    return df.withColumn(name, f.lit(value))
  return _

def attach_modified_date(column = "_MODIFIED"):
  def _(df):
    return df.withColumn(column, f.current_timestamp())
  return _

def attach_partition_column(column, src_pattern = None, name = "_PART"):
  def _(df):    
    if column not in df.columns:
      raise Exception("Column " + column + " is not present in the dataset")

    return (
      df
      .withColumn(name, f.date_trunc('month', f.to_date(f.col(column), src_pattern)).cast('date'))
      .withColumn(name, f.when(f.col(name).isNull(),f.to_date(f.lit('1900-01-01'),src_pattern)).otherwise(f.col(name)))
    )

  return _

def attach_source_column(value, column = "_SOURCE"):
  def _(df):
    return df.withColumn(column, f.lit(value))   
  return _

def attach_subset_column(value, column = "_SUBSET"):
  def _(df):
    return df.withColumn(column, f.lit(value))
  return _

def attach_surrogate_key(columns, name = "_ID", default = '0'):
  
  column_list = _get_column_list(columns)
  
  def _(df):
    return (
      df
      .withColumn(
        name, 
        f.when(sum(f.col(col).isNull().cast('int') for col in column_list) > 0, f.lit(default))
        .otherwise(f.sha2(f.concat_ws('||', *(f.col(col).cast("string") for col in column_list)), 256))
      )
    )
  return _

def attach_unknown_record(df):
  record_df = get_unknown_record(df.schema, default = "Unknown")
  return record_df.union(df)

def cast_all_null_types(data_type = "string"):  
  def _(df):    
    null_columns = _get_columns_by_type(df, ['null'])  
    for column in null_columns:
      df = df.withColumn(column, f.col(column).cast(data_type))
    return df
  return _

def cast_data_type(columns = None, data_type = "string"):
  
  column_list = _get_column_list(columns)
  
  def _(df):    
    nonlocal column_list    
    if column_list is None:
      column_list = df.columns
    
    for column in column_list:
      df = df.withColumn(column, f.col(column).cast(data_type))
    return df
  return _

def change_to_lower_case(columns = None):  

  column_list = _get_column_list(columns)
  
  def _(df):    
    nonlocal column_list    
    if column_list is None:
      column_list = df.columns
    
    for column in column_list:
      df = df.withColumn(column, f.lower(f.col(column)))
    return df
  return _

def change_to_title_case(columns = None):
  
  def change_to_title(c):
    if isinstance(c, type(None)):
      return c
    return c.title()
  change_to_title_udf = udf(change_to_title, StringType())
  
  column_list = _get_column_list(columns)
  
  def _(df):
    nonlocal column_list    
    if column_list is None:
      column_list = df.columns
    
    for column in column_list:
      if dict(df.dtypes)[column] == 'string':
        df = df.withColumn(column, change_to_title_udf(f.col(column)))
    return df
  return _

def change_to_upper_case(columns = None):  

  column_list = _get_column_list(columns)
  
  def _(df):    
    nonlocal column_list    
    if column_list is None:
      column_list = df.columns
    
    for column in column_list:
      df = df.withColumn(column, f.upper(f.col(column)))
    return df
  return _

def clean_multiple_spaces(columns = None):    
  
  column_list = _get_column_list(columns)
  
  def _(df):
    nonlocal column_list
    if column_list is None:
      column_list = df.columns
    
    for column in column_list:
      df = df.withColumn(column, f.regexp_replace(f.trim(f.col(column)), '\\s+', ' '))
    return df
  return _

def clean_unknown_values(columns = None, values = None):
  
  column_list = _get_column_list(columns)
  
  if values is None:
    values = ['', '-', 'n/a', 'not provided', 'not avail', 'not provided']
      
  def _(df):
    nonlocal column_list
    if column_list is None:
      column_list = df.columns
      
    for column in column_list:
      df = df.withColumn(column, 
        f.when(f.trim(f.lower(f.col(column))).isin(values), None)
        .when(f.trim(f.col(column)).isin(values), None)               
        .otherwise(f.col(column)))
    return df
  return _

def clean_unknown_val(columns = None, values = None):
  
  column_list = _get_column_list(columns)
  
  if values is None:
    values = ['', '-', 'not provided', 'not avail', 'not provided']
      
  def _(df):
    nonlocal column_list
    if column_list is None:
      column_list = df.columns
      
    for column in column_list:
      df = df.withColumn(column, 
        f.when(f.trim(f.lower(f.col(column))).isin(values), None)
        .when(f.trim(f.col(column)).isin(values), None)               
        .otherwise(f.col(column)))
    return df
  return _

def convert_empty_string_to_null(columns = None):
  
  column_list = _get_column_list(columns)
  
  def _(df):
    nonlocal column_list    
    if column_list is None:
      column_list = df.columns
      
    for column in column_list:
      df = df.withColumn(column,
        f.when(f.trim(f.col(column)) == '', None)
        .otherwise(f.col(column)))
    return df
  return _

def convert_epoch_to_timestamp(columns = None):
  
  column_list = _get_column_list(columns)
  
  def _(df):
    nonlocal column_list
    if column_list is None:
      column_list = df.columns
      
    for column in column_list:
      df = df.withColumn(column, f.from_unixtime(f.col(column) / 1000).cast('timestamp'))
    return df
  return _

def convert_nan_to_null(columns = None):
  
  column_list = _get_column_list(columns)
  
  def _(df):
    nonlocal column_list    
    if column_list is None:
      column_list = df.columns
      
    for column in column_list:
      df = df.withColumn(column,
        f.when(f.isnan(f.col(column)), None)
        .otherwise(f.col(column)))
    return df
  return _

def convert_null_string_to_null(columns = None):
  
  column_list = _get_column_list(columns)
  
  def _(df):
    nonlocal column_list    
    if column_list is None:
      column_list = df.columns
      
    for column in column_list:
      df = df.withColumn(column, 
        f.when(f.trim(f.col(column)) == 'null', None)
        .otherwise(f.col(column)))
    return df
  return _

def convert_null_to_unknown(columns = None):
  
  column_list = _get_column_list(columns)
    
  def _(df):
    nonlocal column_list    
    if column_list is None:
      column_list = df.columns
    
    for column in column_list:
      df = df.withColumn(column,
        f.when(f.col(column).isNull(), 'Unknown')
        .otherwise(f.col(column)))
    return df
  return _

def convert_null_to_zero(columns = None):
  
  column_list = _get_column_list(columns)
  
  def _(df):
    nonlocal column_list    
    if column_list is None:
      column_list = df.columns
      
    for column in column_list:
      df = df.withColumn(column,
        f.when(f.col(column).isNull(), '0')
        .otherwise(f.col(column)))
    return df
  return _

def drop_duplicate_columns(df):
  columns_renamed = [] 
  seen = set()
  for c in df.columns:
      columns_renamed.append('{}_dup'.format(c) if c in seen else c)
      seen.add(c)

  columns = [c for c in columns_renamed if not c.endswith('_dup')]
  return df.toDF(*columns_renamed).select(*columns)

def fix_cell_values(columns = None):
  
  column_list = _get_column_list(columns)
  
  def _(df):
    nonlocal column_list    
    if column_list is None:
      column_list = df.columns
      
    return (
      df.transform(clean_unknown_values(column_list))
      .transform(clean_multiple_spaces(column_list))
      .transform(convert_empty_string_to_null(column_list))
    )
  return _

def fix_cell_val(columns = None):
  
  column_list = _get_column_list(columns)
  
  def _(df):
    nonlocal column_list    
    if column_list is None:
      column_list = df.columns
      
    return (
      df.transform(clean_unknown_val(column_list))
      .transform(clean_multiple_spaces(column_list))
      .transform(convert_empty_string_to_null(column_list))
    )
  return _

def fix_column_names(df):
  column_names = [f.col("`" + col + "`").alias(re.sub(r'[^A-Za-z0-9_]+', '', col)) for col in df.columns]
  return df.select(column_names)

def fix_dates(columns = None):  
  column_list = _get_column_list(columns)
  
  def _(df):
    nonlocal column_list    
    if column_list is None:
      column_list = _get_columns_by_type(df, data_type = ['date', 'timestamp'])
   
    for column in column_list:
      df = (
        df.withColumn(column, 
          f.when(f.col(column) < '1900-01-01', None)
          .otherwise(f.col(column))))
    return df
  return _

def get_unknown_record(schema, default = "Unknown"):
  row = spark.range(1).drop("id")
  for column in schema:
    if column.name == "_SOURCE":
      row = row.withColumn("_SOURCE", f.lit("DEFAULT"))      
    elif column.name == "_DELETED":
      row = row.withColumn("_DELETED", f.lit(False))
    elif column.name == "_MODIFIED":
      row = row.withColumn("_MODIFIED", f.current_timestamp())  
    elif str(column.name)[-3:] == '_ID':
      row = row.withColumn(column.name, f.lit(0))    
    elif str(column.dataType) == 'StringType':
      row = row.withColumn(column.name, f.lit(default))
    elif column.name == "_PART":
      row = row.withColumn("_PART", f.to_date(f.lit('1900-01-01'),None)) 
    else:
      row = row.withColumn(column.name, f.lit(None))      
  return row

def overwrite_value(columns = None):    
  
  column_list = _get_column_list(columns)
  
  def _(df):
    nonlocal column_list
    if column_list is None:
      column_list = df.columns
    
    for column in column_list:
      df = df.withColumn(column, f.lit('***'))
    return df
  return _

def parse_date(columns = None, expected_format = ['yyyy-MM-dd']):
  column_list = _get_column_list(columns)
  
  def _(df):
    nonlocal column_list    
    if column_list is None:
      column_list = _get_columns_by_type(df, data_type = ['date'])
   
    for column in column_list:
      for date_format in expected_format:
        df = df.withColumn(column,
          f.when(f.to_date(f.col(column), date_format).isNotNull(), f.to_date(f.col(column), date_format))
          .otherwise(f.col(column))
        )
    return df
  return _

def parse_timestamp(columns = None, expected_format = ['yyyy-MM-dd HH:mm:ss']):
  column_list = _get_column_list(columns)
  
  def _(df):
    nonlocal column_list    
    if column_list is None:
      column_list = _get_columns_by_type(df, data_type = ['timestamp'])
   
    for column in column_list:
      for date_format in expected_format:
        df = df.withColumn(column,
          f.when(f.to_timestamp(f.col(column), date_format).isNotNull(), f.to_timestamp(f.col(column), date_format))
          .otherwise(f.col(column))
        )
    return df
  return _

def rename_columns(columns = None):  
  def _(df):
    nonlocal columns
    return df.select(*[f.col(col_name).alias(columns.get(col_name, col_name)) for col_name in df.columns])
  return _

def replace_character(old_char, new_char, columns = None):
  column_list = _get_column_list(columns)
  
  replace_character_udf = udf(lambda x: x if (x is None) else x.replace(old_char, new_char), StringType())
  def _(df):
    nonlocal column_list
    if column_list is None:
      column_list = _get_columns_by_type(df, data_type = ['string'])
    
    for column in column_list:
      df = df.withColumn(column, replace_character_udf(f.col(column)))
    return df
  return _

def sort_columns(df):
  
  key_columns = [c for c in df.columns if c == '_ID' or c[-3:] == '_ID']
  key_columns.sort()
  
  internal_columns = [c for c in df.columns if c[:1] == '_' and c != '_ID']
  internal_columns.sort()
 
  special_columns = key_columns + internal_columns
  data_columns = [c for c in df.columns if (c not in special_columns)]
  
  return df.select(special_columns + data_columns)

def trim_all_values(df):
  str_cols = [dtype[0] for dtype in df.dtypes if dtype[1] == 'string']
  for column in str_cols:
    df = df.withColumn(column, f.trim(f.col(column)))
  return df

# COMMAND ----------

def _get_column_list(columns):
  column_list = columns
  if isinstance(columns, str):
    column_list = [value.strip() for value in columns.split(',')]
  return column_list

def _get_columns_by_type(df, data_type = ['string']):
  return [dt[0] for dt in df.dtypes if dt[1] in data_type] 

# COMMAND ----------

# UDF
def days_between(x, y):
  return 0 if (x is None) | (y is None) else int(np.busday_count(x, y))
days_between_udf = f.udf(days_between, IntegerType())

def days_till_today(x):
  today = date.today() + timedelta(days=1)
  return 0 if x is None else int(np.busday_count(x, today.strftime("%Y-%m-%d")))
days_till_today_udf = f.udf(days_till_today, IntegerType())

# COMMAND ----------

def filter_null_values(columns):
    def _(df):   
        if isinstance(columns, str):       
            hash_cols = [column.strip() for column in columns.split(',')]
        return df.na.drop(subset=hash_cols)
    return _
