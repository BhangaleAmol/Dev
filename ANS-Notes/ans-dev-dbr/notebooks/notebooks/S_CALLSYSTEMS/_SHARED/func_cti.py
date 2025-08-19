# Databricks notebook source
# MAGIC %run ../../_SHARED/FUNCTIONS/1.3/func_transformation

# COMMAND ----------

def cast_to_int(columns):
  def inner(df):
    for c_name in columns:
      df = df.withColumn(c_name, f.regexp_replace(df[c_name], "[^0-9]", "").cast(IntegerType()))
    return df
  return inner
      
def drop_columns(columns):
  def inner(df):
    for c_name in columns:
      df = df.drop(c_name)
    return df
  return inner

def epoch_to_timestamp(columns):
  def inner(df):
    for c_name in columns:
      df = df.withColumn(c_name, f.from_unixtime(f.col(c_name) / 1000))
    return df
  return inner

def rename_column(column, name):
  def inner(df):
    return df.withColumnRenamed(column, name)
  return inner

def rename_column_to_title(separator):
  def inner(df):
    for c_name in df.columns:
      if c_name[0] == "_":
        continue
      new_name = re.sub(r'(__[a-z])', '', c_name)
      new_name = ''.join(map(lambda x: x if x.islower() else separator + x, new_name))
      new_name = new_name.upper()
      df = df.withColumnRenamed(c_name, new_name)
    return df  
  return inner

def replace_na_string_with_null(df):
  return df.replace("NA", None).replace('na', None)

def replace_negative_with_zero(columns):
  def inner(df):
    for c in columns:
      df = df.withColumn(c, 
        f.when(f.col(c) < 0, 0)
        .otherwise(f.col(c))
      )
    return df
  return inner

def replace_null_string_with_null(df):
  return df.replace("null", None)

def replace_nulls_with_unknown(columns):
  def inner(df):
    return df.na.fill("Unknown", columns)
  return inner

def replace_special_chars(columns, old_char, new_char):
  replace_udf = udf(lambda x: x if (x is None) else x.replace(old_char, new_char), StringType())
  def inner(df):
    for c_name in columns:
      df = df.withColumn(c_name, replace_udf(f.col(c_name)))
    return df
  return inner

def clean_phone_numbers(columns):
  def inner(df):
    for c_name in columns:
      df = df.withColumn(c_name, f.regexp_replace(c_name, r'^0+', ""))
      df = df.withColumn(c_name, f.regexp_replace(c_name, r'[^0-9]', ""))
      df = df.withColumn(c_name, f.when(f.col(c_name) == 'anonymous', None).otherwise(f.col(c_name)))
      df = df.withColumn(c_name, f.when(f.col(c_name) == 'Unavailable', None).otherwise(f.col(c_name)))
      df = df.withColumn(c_name, f.when(f.col(c_name) == '', None).otherwise(f.col(c_name)))
      df = df.withColumn(c_name, f.when(f.col(c_name) == '0', None).otherwise(f.col(c_name)))
      df = df.withColumn(c_name, f.when(f.length(f.col(c_name)) < 5, None).otherwise(f.col(c_name)))
      df = df.withColumn(c_name, f.when(f.col(c_name).isNull(), None).otherwise(f.col(c_name)))
    return df
  return inner
