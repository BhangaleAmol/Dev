# Databricks notebook source
def show_duplicate_rows(df, fields):
  
  if isinstance(fields, str):
    fields = [value.strip() for value in fields.split(',')]
  
  duplicates = df.join(
    df.groupBy(fields).count().where('count = 1').drop('count'), 
    on=fields,
    how='left_anti'
  ).orderBy(fields)  
  display(duplicates)

def show_empty_rows(df, fields):
  
  if isinstance(fields, str):
    fields = [value.strip() for value in fields.split(',')]  

  filter_str = ' or '.join(["((trim({0}) == '') or ({0} is null))".format(field) for field in fields])
  result = df.filter(filter_str)
  display(result)  
  
def show_empty_string_values(df):
  result = df.select([f.count(f.when(f.col(c) == '', c)).alias(c) for c in df.columns])
  display(result)
  
def show_null_values(df, columns = None):
  if columns is None:
    columns = df.columns
  
  elif isinstance(columns, str):
    columns = [value.strip() for value in columns.split(',')]  
    
  result = df.select([f.count(f.when(f.col(c).isNull(), c)).alias(c) for c in columns])
  display(result)

# COMMAND ----------

def valid_count_rows(df, fields, dataset_name = 'DATASET'):
  
  if isinstance(fields, str):
    fields = [value.strip() for value in fields.split(',')]  
 
  countTotal = df.select(fields).count()
  countDistinct = df.select(fields).distinct().count()

  if(countTotal != countDistinct):
    message = 'COUNT TOTAL ({0}) COUNT DISTINCT ({1}) FOR {2} IN {3} '.format(countTotal, countDistinct, str(fields), dataset_name)
    raise Exception(message)
  else:
    message = '(OK) VALID COUNT ROWS ({0}) FOR {1} IN {2} '.format(countTotal, str(fields), dataset_name)
    print(message)
