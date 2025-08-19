# Databricks notebook source
# EXTEND DATAFRAME OBJECT
def transform(self, f):
  return f(self)
DataFrame.transform = transform

# COMMAND ----------

def apply_schema(schema):
  def inner(df):
    for column, data_type in schema.items():
      if column in df.columns:
        df = df.withColumn(column, f.col(column).cast(data_type))

    columns = [data_type[0] for data_type in df.dtypes if data_type[1] == 'null']    
    for column in columns:
      df = df.withColumn(column, f.col(column).cast("string"))
    return df
  return inner

def attach_archive_column(column_name = "_ARCHIVE", archive_type = "m"):
  def inner(df):
   
    date_pattern = {
      'm': "yyyy-MM", 
      'd': "yyyy-MM-dd"    
    }[archive_type.lower()]

    df = df.withColumn(column_name, f.date_format(f.current_date(), date_pattern))
    return df
  return inner

def attach_deleted_flag(value, name = "_DELETED"):
  def inner(df):
    return df.withColumn(name, f.lit(False))
  return inner

def attach_partition_column(column, src_pattern = None, tgt_pattern = 'yyyy-MM', part_name = "_PART"):
  def inner(df):    
    if column not in df.columns:
      raise Exception("Column " + column + " is not present in the dataset")
    else:
      df = df.withColumn(part_name, f.date_format(f.to_date(df[column], src_pattern), tgt_pattern))
    return df
  return inner

def attach_source_column(source, column = "_SOURCE"):
  def inner(df):
    df = df.withColumn(column, f.lit(source))
    return df
  return inner

def attach_surrogate_key(columns, name = "_ID", default = '0'):
  def inner(df):    
    hash_cols = [column.strip() for column in columns.split(',')]        
    return df.withColumn(
      name, 
      f.when(sum(f.col(col).isNull().cast('int') for col in hash_cols) > 0, f.lit(default))
      .otherwise(f.sha2(f.concat_ws('||', *(f.col(col).cast("string") for col in hash_cols)), 256))
    )
  return inner

def cast_all_null_types(data_type = "string"):  
  def inner(df):    
    columns = [data_type[0] for data_type in df.dtypes if data_type[1] == 'null']    
    for column in columns:
      df = df.withColumn(column, f.col(column).cast(data_type))
    return df
  return inner

def cast_data_type(columns, data_type = "string"):
  def inner(df):
    for column in columns:
      df = df.withColumn(column, f.col(column).cast(data_type))
    return df
  return inner

def change_to_title_case(columns):
  
  def change_to_title(c):
    return c.title()
  change_to_title_udf = udf(change_to_title, StringType())
  
  def inner(df):
    for column in columns:
      df = df.withColumn(column, change_to_title_udf(f.col(column)))
    return df
  return inner

def change_to_upper_case(columns):  
  def inner(df):
    for column in columns:
      df = df.withColumn(column, f.upper(f.col(column)))
    return df
  return inner

def clean_empty_string_to_null(columns):
  def inner(df):
    for column in columns:
      df = df.withColumn(column,f.when(f.trim(f.col(column)) == '', None).otherwise(f.col(column)))
    return df
  return inner

def clean_multiple_spaces(columns):
  def inner(df):
    for column in columns:
      df = df.withColumn(column, f.regexp_replace(f.trim(f.col(column)), '\\s+', ' '))
    return df
  return inner

def clean_unknown_values(columns, values = None):
  if values is None:
      values = ['', '-', 'n/a', 'not provided', 'not avail', 'not provided']
      
  def inner(df):
    for column in columns:
      df = df.withColumn(column, f.when(f.trim(f.lower(f.col(column))).isin(values), None).otherwise(f.col(column)))
    return df
  return inner

def convert_empty_string_to_null(df):
  for column in df.columns:
    df = df.withColumn(column,f.when(f.trim(f.col(column)) == '', None).otherwise(f.col(column)))
  return df

def convert_nan_to_null(df):
  for column in df.columns:
    df = df.withColumn(column,f.when(f.isnan(f.col(column)), None).otherwise(f.col(column)))
  return df

def convert_null_to_zero(columns):
  def inner(df):
    for column in columns:
      df = df.withColumn(column,f.when(f.col(column).isNull(), '0').otherwise(f.col(column)))
    return df
  return inner

def fix_cell_values(columns):
  def inner(df):
    df = df.transform(clean_unknown_values(columns))
    df = df.transform(clean_multiple_spaces(columns))
    df = df.transform(clean_empty_string_to_null(columns))
    return df
  return inner

def fix_column_names(df):
  df = df.select([f.col("`" + col + "`").alias(re.sub(r'[^A-Za-z0-9_]+', '', col))  for col in df.columns])
  return df

def fix_dates(df):
  date_cols = [dtype[0] for dtype in df.dtypes if dtype[1] == 'date' or dtype[1] == 'timestamp']
  for column in date_cols:
    df = df.withColumn(column, f.when(f.col(column) < '1900-01-01', None).otherwise(f.col(column)))
  return df

def get_unknown_record(schema, default = "unknown"):
    
  row = spark.range(1).drop("id")  
  for column in schema:    
    # source    
    if column.name == "_SOURCE":
      row = row.withColumn("_SOURCE", f.lit("DEFAULT"))    
    # keys
    elif str(column.name)[-3:] == '_ID':
      row = row.withColumn(column.name, f.lit(0))    
    # string fields
    elif str(column.dataType) == 'StringType':
      row = row.withColumn(column.name, f.lit(default))
    else:
      row = row.withColumn(column.name, f.lit(None))
      
  return row

def trim_all_values(df):
  str_cols = [dtype[0] for dtype in df.dtypes if dtype[1] == 'string']
  for column in str_cols:
    df = df.withColumn(column, f.trim(f.col(column)))
  return df

# COMMAND ----------

def days_between(x, y):
  return 0 if (x is None) | (y is None) else int(np.busday_count(x, y))
days_between_udf = f.udf(days_between, IntegerType())

def days_till_today(x):
  today = date.today() + datetime.timedelta(days=1)
  return 0 if x is None else int(np.busday_count(x, today.strftime("%Y-%m-%d")))
days_till_today_udf = f.udf(days_till_today, IntegerType())
