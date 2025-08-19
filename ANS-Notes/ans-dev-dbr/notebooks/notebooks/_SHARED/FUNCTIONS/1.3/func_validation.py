# Databricks notebook source
def check_deleted_count(df, threshold = 50):
  df.cache()
  count_total = df.count()
  count_del = df.filter('_DELETED IS TRUE').count()
  
  if count_total == 0:
    perc_deleted = 0
  else:
    perc_deleted = round((count_del / count_total) * 100)
    
  if (perc_deleted > threshold):
    message = 'DELETED RECORDS {0}% ABOVE {1}% THRESHOLD'.format(perc_deleted, threshold)
    raise Exception(message)
  else:
    message = '(OK) DELETED RECORDS {0}% BELOW {1}% THRESHOLD'.format(perc_deleted, threshold)
    print(message)

def check_distinct_count(df, fields):
  
  if isinstance(fields, str):
    fields = [value.strip() for value in fields.split(',')]  
  
  fields_df = df.select(fields)
  fields_df.cache()
  
  count_total = fields_df.count()
  count_dist = fields_df.distinct().count()

  if(count_total != count_dist):
    message = 'COUNT TOTAL ({0}) COUNT DISTINCT ({1}) FOR {2}'.format(count_total, count_dist, str(fields))
    raise Exception(message)
  else:
    message = '(OK) VALID COUNT ROWS ({0}) FOR {1}'.format(count_total, str(fields))
    print(message)

def check_not_null(df, fields):
  
  if isinstance(fields, str):
    fields = [value.strip() for value in fields.split(',')]  
  
  fields_df = df.select(fields)
  fields_df.cache()
  
  count_not_null = fields_df.na.drop("any").count()
  count_total = fields_df.count()

  if(count_total != count_not_null):
    message = f'ROWS WITH NULL VALUE ({count_total - count_not_null}) FOR {str(fields)}'
    raise Exception(message)
  else:
    message = f'(OK) NO NULL VALUES FOR {str(fields)}'
    print(message)    

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
    
def get_duplicate_rows(df, fields):
  
  if isinstance(fields, str):
    fields = [value.strip() for value in fields.split(',')]
  
  duplicates = df.join(
    df.groupBy(fields).count().where('count = 1').drop('count'), 
    on=fields,
    how='left_anti'
  ).orderBy(fields)  
  return duplicates

# COMMAND ----------

def remove_duplicate_rows(df, key_columns, tableName, sourceName, notebookName, notebookPath):

  if key_columns is not None:
    duplicates = get_duplicate_rows(df, key_columns)
    duplicates.cache()

    if not duplicates.rdd.isEmpty():
      notebook_data = {
        'source_name': sourceName,
        'notebook_name': notebookName,
        'notebook_path': notebookPath,
        'target_name': tableName,
        'duplicates_count': duplicates.count(),
        'duplicates_sample': duplicates.select(key_columns).limit(50)
      }
      send_mail_duplicate_records_found(notebook_data)
      df = df.dropDuplicates(subset = key_columns)      
  
  return df
