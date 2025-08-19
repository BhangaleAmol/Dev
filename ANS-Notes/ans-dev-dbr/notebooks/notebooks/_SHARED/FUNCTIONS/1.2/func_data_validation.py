# Databricks notebook source
def get_duplicate_rows(df, fields, options = {}):
  
  distinct = options.get('distinct', False)
  full_record = options.get('full_record', False)
  ordered = options.get('ordered', False)
  
  if isinstance(fields, str):
    fields = [value.strip() for value in fields.split(',')]
  
  duplicates_df = (
    df
    .join(
      df.groupBy(fields).count().where('count = 1').drop('count'), 
      on=fields,
      how='left_anti'
    ) 
  )
  
  duplicates_df = duplicates_df if full_record else duplicates_df.select(fields)
  duplicates_df = duplicates_df if not ordered else duplicates_df.orderBy(fields)
  duplicates_df = duplicates_df if not distinct else duplicates_df.distinct()
  
  return duplicates_df

def show_duplicate_rows(df, fields, options = {}):  
  get_duplicate_rows(df, fields, options).display()

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
  
  fields_df = df.select(fields)
  fields_df.cache()
  
  countTotal = fields_df.count()
  countDistinct = fields_df.distinct().count()

  if(countTotal != countDistinct):
    message = 'COUNT TOTAL ({0}) COUNT DISTINCT ({1}) FOR {2} IN {3} '.format(countTotal, countDistinct, str(fields), dataset_name)
    raise Exception(message)
  else:
    message = '(OK) VALID COUNT ROWS ({0}) FOR {1} IN {2} '.format(countTotal, str(fields), dataset_name)
    print(message)

# COMMAND ----------

def check_row_count(table_name, full_keys_df, threshold = 0.05):

  table_df = spark.table(table_name)
  
  if '_DELETED' in table_df.columns:
    table_df = table_df.filter('not _DELETED')
    
  table_keys_count = table_df.count()
  full_keys_count = full_keys_df.count()
  
  print(f'table rows: {table_keys_count}')
  print(f'full keys rows: {full_keys_count}')
  
  threshold_count = int(table_keys_count + table_keys_count * threshold)
  if (threshold_count < full_keys_count):
    notebook_data = {   
      'target_name': table_name,
      'table_keys_count': table_keys_count,
      'full_keys_count': full_keys_count
    }
    send_mail_row_count_mismatch(notebook_data) 

# COMMAND ----------

def remove_duplicate_rows(df, key_columns, tableName, sourceName, notebookName, notebookPath):

  if key_columns is not None:
    duplicates = get_duplicate_rows(df, key_columns)
    duplicates.cache()

    if not duplicates.rdd.isEmpty():
      duplicates_count = duplicates.count()
      notebook_data = {
        'source_name': sourceName,
        'notebook_name': notebookName,
        'notebook_path': notebookPath,
        'target_name': tableName,
        'duplicates_count': duplicates_count,
        'duplicates_sample': duplicates.select(key_columns).limit(50)
      }
      print(f'duplicates found: {duplicates_count}')
      send_mail_duplicate_records_found(notebook_data)
      df = df.dropDuplicates(subset = key_columns)      
  
  return df
