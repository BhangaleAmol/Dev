# Databricks notebook source
# EXTEND DATAFRAME OBJECT
def transform(self, f):
  return f(self)
DataFrame.transform = transform

def transform_if(self, cond: bool ,f):
  if cond is True:
    return f(self)
  else:
    return self
DataFrame.transform_if = transform_if  

# COMMAND ----------

def apply_schema(schema):
  def inner(df):
    df_columns = [c.lower() for c in df.columns]
    for column, data_type in schema.items():
      if column.lower() in df_columns:
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

def attach_dataset_column(value, name = "_DATASET"):
  def inner(df):    
    return df.withColumn(name, f.lit(value))      
  return inner

def attach_deleted_flag(value, name = "_DELETED"):
  def inner(df):
    return df.withColumn(name, f.lit(False))
  return inner

def attach_modified_date(column = "_MODIFIED"):
  def inner(df):
    return df.withColumn(column, f.current_timestamp())
  return inner

def attach_partition_column(column, src_pattern = 'yyyy-MM-ddTHH:mm:ss', tgt_pattern = 'yyyy-MM', part_name = "_PART"):
  def inner(df):    
    if column not in df.columns:
      raise Exception("Column " + column + " is not present in the dataset")
    
    return (
      df
      .withColumn(
        part_name, 
        f.date_format(f.to_date(df[column], src_pattern), tgt_pattern)
      )
      .withColumn(
        part_name, 
        f.when(f.col(part_name).isNull(),'1900-01-01').otherwise(f.col(part_name))
      )      
    )

  return inner

def attach_source_column(source, column = "_SOURCE"):
  def inner(df):    
    return df.withColumn(column, f.lit(source))   
  return inner

def attach_surrogate_key(columns, name = "_ID", default = '0'):
  def inner(df):
    nonlocal columns
    
    if isinstance(columns, str):       
      columns = [column.strip() for column in columns.split(',')]
      
    return (
      df
      .withColumn(
        name, 
        f.when(sum(f.col(col).isNull().cast('int') for col in columns) > 0, f.lit(default))
        .otherwise(f.sha2(f.concat_ws('||', *(f.col(col).cast("string") for col in columns)), 256))
      )
    )
  return inner

def attach_unknown_record(df):
  unknown_df = get_unknown_record(df.schema)
  return df.union(unknown_df)

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

def clean_unknown_val(columns, values = None):
  if values is None:
      values = ['', '-', 'not provided', 'not avail', 'not provided']
      
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


def convert_null_to_unknown(columns):
  def inner(df):
    nonlocal columns
    if isinstance(columns, str):    
      columns = [column.strip() for column in columns.split(',')]  
    for column in columns:
      df = df.withColumn(column,f.when(f.col(column).isNull(), 'Unknown').otherwise(f.col(column)))
    return df
  return inner

def convert_null_to_false(columns):
  def inner(df):
    nonlocal columns
    if isinstance(columns, str):    
      columns = [column.strip() for column in columns.split(',')]  
    for column in columns:
      df = df.withColumn(column,f.when(f.col(column).isNull(), 'false').otherwise(f.col(column)))
    return df
  return inner

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

def fix_cell_val(columns):
  def inner(df):
    df = df.transform(clean_unknown_val(columns))
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
    elif column.name == "_DELETED":
      row = row.withColumn("_DELETED", f.lit(False))
    # keys
    elif str(column.name)[-3:] == '_ID':
      row = row.withColumn(column.name, f.lit(0))    
    # string fields
    elif str(column.dataType) == 'StringType':
      row = row.withColumn(column.name, f.lit(default))
    elif column.name == "_PART":
      row = row.withColumn("_PART", f.lit('1900-01-01'))
    else:
      row = row.withColumn(column.name, f.lit(None))
      
  return row

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

def days_between(x, y):
  return 0 if (x is None) | (y is None) else int(np.busday_count(x, y))
days_between_udf = f.udf(days_between, IntegerType())

def days_till_today(x):
  today = date.today() + datetime.timedelta(days=1)
  return 0 if x is None else int(np.busday_count(x, today.strftime("%Y-%m-%d")))
days_till_today_udf = f.udf(days_till_today, IntegerType())

# COMMAND ----------

def parse_date(columns, expected_format = 'yyyy-MM-dd'):
  def inner(df):
    for column in columns:
     
      if expected_format[0] == 'y':
        df = df.withColumn(column,
          f.when(f.to_date(f.col(column), expected_format).isNotNull(), f.to_date(f.col(column), expected_format))
          .when(f.to_date(f.col(column), "yyyy-MM-dd").isNotNull(), f.to_date(f.col(column), "yyyy-MM-dd"))
          .when(f.to_date(f.col(column), "yyyy MM dd").isNotNull(), f.to_date(f.col(column), "yyyy MM dd"))
          .when(f.to_date(f.col(column), "yyyy/MM/dd").isNotNull(), f.to_date(f.col(column), "yyyy/MM/dd"))
          .when(f.to_date(f.col(column), "yyyy-MMM-dd").isNotNull() & (f.length(f.trim(f.col(column))) == 11) & (f.trim(f.col(column)).contains('-')), f.to_date(f.col(column), "yyyy-MMM-dd"))
          .when(f.to_date(f.col(column), "yyyy/MM/dd").isNotNull() & (f.length(f.trim(f.col(column))) == 10) & (f.trim(f.col(column)).contains('/')), f.to_date(f.col(column), "yyyy/MM/dd"))
          .when(f.to_date(f.col(column), "dd/MMM/yy").isNotNull() & (f.length(f.trim(f.col(column))) == 9) & (f.trim(f.col(column)).contains('/')), f.to_date(f.col(column), "dd/MMM/yy"))
          .when(f.to_date(f.col(column), "dd-MMM-yy").isNotNull()  & (f.length(f.trim(f.col(column))) == 9) & (f.trim(f.col(column)).contains('-')) , f.to_date(f.col(column), "dd-MMM-yy"))
          .otherwise(None)
        )
     
      if expected_format[0] == 'd':
        df = df.withColumn(column,
          f.when(f.to_date(f.col(column), expected_format).isNotNull(), f.to_date(f.col(column), expected_format))
          .when(f.to_date(f.col(column), "dd/MM/yy").isNotNull(), f.to_date(f.col(column), "dd/MM/yy"))
          .when(f.to_date(f.col(column), "dd/MM/yyyy").isNotNull(), f.to_date(f.col(column), "dd/MM/yyyy"))
          .when(f.to_date(f.col(column), "d/M/yy").isNotNull(), f.to_date(f.col(column), "d/M/yy"))
          .when(f.to_date(f.col(column), "d/M/yyyy").isNotNull(), f.to_date(f.col(column), "d/M/yyyy"))
          .otherwise(None)
        )      
                             
      if expected_format[0] == 'M':
        df = df.withColumn(column,
          f.when(f.to_date(f.col(column), expected_format).isNotNull(), f.to_date(f.col(column), expected_format))
          .when(f.to_date(f.col(column), "MM/dd/yy").isNotNull(), f.to_date(f.col(column), "MM/dd/yy"))
          .when(f.to_date(f.col(column), "MM/dd/yyyy").isNotNull(), f.to_date(f.col(column), "MM/dd/yyyy"))                  
          .when(f.to_date(f.col(column), "M/d/yy").isNotNull(), f.to_date(f.col(column), "M/d/yy"))
          .when(f.to_date(f.col(column), "M/d/yyyy").isNotNull(), f.to_date(f.col(column), "M/d/yyyy"))  
          .when(f.to_date(f.col(column), "M/dd/yy").isNotNull(), f.to_date(f.col(column), "M/dd/yy"))
          .when(f.to_date(f.col(column), "M/dd/yyyy").isNotNull(), f.to_date(f.col(column), "M/dd/yyyy"))                     
          .otherwise(None)
        )
    return df
     
  return inner  

# COMMAND ----------

def parse_timestamp(columns, expected_format = 'yyyy-MM-dd'):
  def inner(df):
    for column in columns:
     
      if expected_format[0] == 'y':
        df = df.withColumn(column,
          f.when(f.to_timestamp(f.col(column), expected_format).isNotNull(), f.to_timestamp(f.col(column), expected_format))
          .when(f.to_timestamp(f.col(column), "yyyy-MM-dd hh:mm:ss").isNotNull(), f.to_timestamp(f.col(column), "yyyy-MM-dd hh:mm:ss"))
          .when(f.to_timestamp(f.col(column), "yyyy MM dd hh:mm:ss").isNotNull(), f.to_timestamp(f.col(column), "yyyy MM dd hh:mm:ss"))
          .when(f.to_timestamp(f.col(column), "yyyy/MM/dd hh:mm:ss").isNotNull(), f.to_timestamp(f.col(column), "yyyy/MM/dd hh:mm:ss")) 
          .otherwise(None)
        )
     
      if expected_format[0] == 'd':
        df = df.withColumn(column,
          f.when(f.to_timestamp(f.col(column), expected_format).isNotNull(), f.to_timestamp(f.col(column), expected_format))
          .when(f.to_timestamp(f.col(column), "dd/MM/yy hh:mm:ss").isNotNull(), f.to_timestamp(f.col(column), "dd/MM/yy hh:mm:ss"))
          .when(f.to_timestamp(f.col(column), "dd/MM/yyyy hh:mm:ss").isNotNull(), f.to_timestamp(f.col(column), "dd/MM/yyyy hh:mm:ss"))
          .when(f.to_timestamp(f.col(column), "d/M/yy hh:mm:ss").isNotNull(), f.to_timestamp(f.col(column), "d/M/yy hh:mm:ss"))
          .when(f.to_timestamp(f.col(column), "d/M/yyyy hh:mm:ss").isNotNull(), f.to_timestamp(f.col(column), "d/M/yyyy hh:mm:ss"))
          .otherwise(None)
        )      
                             
      if expected_format[0] == 'M':
        df = df.withColumn(column,
          f.when(f.to_timestamp(f.col(column), expected_format).isNotNull(), f.to_timestamp(f.col(column), expected_format))
          .when(f.to_timestamp(f.col(column), "MM/dd/yy hh:mm:ss").isNotNull(), f.to_timestamp(f.col(column), "MM/dd/yy hh:mm:ss"))
          .when(f.to_timestamp(f.col(column), "MM/dd/yyyy hh:mm:ss").isNotNull(), f.to_timestamp(f.col(column), "MM/dd/yyyy hh:mm:ss"))                  
          .when(f.to_timestamp(f.col(column), "M/d/yy hh:mm:ss").isNotNull(), f.to_timestamp(f.col(column), "M/d/yy hh:mm:ss"))
          .when(f.to_timestamp(f.col(column), "M/d/yyyy hh:mm:ss").isNotNull(), f.to_timestamp(f.col(column), "M/d/yyyy hh:mm:ss"))  
          .when(f.to_timestamp(f.col(column), "M/dd/yy hh:mm:ss").isNotNull(), f.to_timestamp(f.col(column), "M/dd/yy hh:mm:ss"))
          .when(f.to_timestamp(f.col(column), "M/dd/yyyy hh:mm:ss").isNotNull(), f.to_timestamp(f.col(column), "M/dd/yyyy hh:mm:ss"))
          .when(f.to_timestamp(f.col(column), "M/d/yyyy hh:mm:ss").isNotNull(), f.to_timestamp(f.col(column), "M/d/yyyy hh:mm:ss"))                     
          .when(f.to_timestamp(f.col(column), "M/dd/yyyy h:mm").isNotNull(), f.to_timestamp(f.col(column), "M/dd/yyyy hh:mm:ss"))
          .when(f.to_timestamp(f.col(column), "M/d/yyyy h:mm").isNotNull(), f.to_timestamp(f.col(column), "M/d/yyyy hh:mm:ss")) 
          .when(f.to_timestamp(f.col(column), "M/dd/yyyy hh:mm").isNotNull(), f.to_timestamp(f.col(column), "M/dd/yyyy hh:mm:ss"))
          .when(f.to_timestamp(f.col(column), "M/d/yyyy hh:mm").isNotNull(), f.to_timestamp(f.col(column), "M/d/yyyy hh:mm:ss"))                  
          .otherwise(None)
        )
    return df     
  return inner  

# COMMAND ----------

def fix_uom_conversions(columns):
  def inner(df):
    hash_col=[column.strip() for column in columns.split(',')]
    for column in hash_col:
      df = df.withColumn(column,f.when(f.col(column) > 0, f.col(column)).otherwise(1))
    return df
  return inner

# COMMAND ----------

def add_unknown_ID():
  def inner(df):
    unknown_ID = spark.createDataFrame([['0']])
    df = df.union(unknown_ID)
    return df
  return inner

# COMMAND ----------

def filter_null_values(columns):
    def inner(df):   
        if isinstance(columns, str):   
            hash_cols = [column.strip() for column in columns.split(',')]
        return df.na.drop(subset=hash_cols)
    return inner

# COMMAND ----------

def edm_stdrd_codes(options = {}):
  def inner(df):
     edm_table_name = options.get('EDM_TABLE_NAME')
     table_name = options.get('TABLE_NAME')
     field_name = options.get('FIELD_NAME')
     new_field  = options.get('NEW_FIELD')
     source = options.get('SOURCE')
     query = "SELECT SOURCE,NEW_VALUE,ORIGINAL_VALUE,FIELD_NAME FROM {0} WHERE TABLE_NAME = '{1}' and ACTIVE = True and FIELD_NAME = '{2}' and SOURCE = '{3}'".format(edm_table_name,table_name,field_name,   
              source)
     print(query)
     edm_std_codes = spark.sql(query)  
     df =  (df
              .join(edm_std_codes, (edm_std_codes.ORIGINAL_VALUE == df[field_name]), 'left')
              .withColumn(new_field, f.coalesce(f.col('NEW_VALUE'),f.col(field_name)))
            )
     return df
  return inner
