# Databricks notebook source
# MAGIC %run ./func_helpers

# COMMAND ----------

# MAGIC %run ./func_azure_table

# COMMAND ----------

from datetime import datetime, timedelta

# COMMAND ----------

def get_cutoff_value(table_name, source_table = None, prune_days = 30,
  incremental_type = 'timestamp', date_format = "%Y-%m-%d"):  
 
  record = {}
  record = get_timestamp_record(table_name, source_table)
  
  # default value
  cutoff_value = '1900-01-01T00:00:00'
  
  # timestamp
  if incremental_type == 'timestamp':
    
    max_datetime = record["MAX_DATETIME"]
    max_datetime_dt = datetime.strptime(max_datetime, '%Y-%m-%dT%H:%M:%SZ')

    cutoff_datetime_dt = datetime.strptime('1900-01-01T00:00:00Z', '%Y-%m-%dT%H:%M:%SZ')
    if max_datetime != '1900-01-01T00:00:00Z' and prune_days is not None:
      cutoff_datetime_dt = max_datetime_dt + timedelta(days=-prune_days)

    cutoff_value = cutoff_datetime_dt.strftime(date_format)
    cutoff_value = cutoff_value.replace('T', ' ')
  
  # sequence
  if incremental_type == 'sequence':
    max_sequence = record["MAX_SEQUENCE"]
    cutoff_value = max_sequence  
 
  return cutoff_value

def get_cutoff_value_from_current_date(prune_days = 30):
  cutoff_value = datetime.today() - timedelta(days = prune_days)
  return cutoff_value.strftime("%Y-%m-%d %H:%M:%S")

def get_max_value(df, incremental_column = None, incremental_type = 'timestamp'):    
  
  max_value = None
  
  if (incremental_type == 'timestamp'):
    default_value = '1900-01-01T00:00:00'
  else: 
    default_value = 0
  
  if incremental_column is None:    
    return default_value
  
  else:

    if incremental_column not in df.columns:
      message = "Column {0} not present in the dataset".format(incremental_column)
      raise Exception(message)    
    
    max_value = df.select(f.max(f.col(incremental_column)).alias("max"))
    
    data_type = str(df.schema[incremental_column].dataType)
    
    if data_type == 'TimestampType':
      max_value = (
        df.select(f.date_format(f.max(f.col(incremental_column)), 'yyyy-MM-dd HH:mm:ss').alias("max"))
        .limit(1).collect()[0].max) 
    
    elif data_type == 'DateType':  
      max_value = (
        df.select(f.date_format(f.max(f.col(incremental_column)), 'yyyy-MM-dd').alias("max"))
        .limit(1).collect()[0].max)
    
    else:
      max_value = (
        df.select(f.max(f.col(incremental_column)).alias("max"))
        .limit(1).collect()[0].max)
          
    if max_value is None:      
      max_value = default_value      
    
    return str(max_value)

def get_timestamp_record(table_name, source_name = None):
  
  database_name, s_table_name = _split_table_name(table_name)
  
  partition_key = database_name.lower()
  row_key = s_table_name if source_name is None else "{0} [{1}]".format(s_table_name, source_name)
  row_key = row_key.lower()
  data = {'PartitionKey': partition_key,'RowKey': row_key}
  
  sas_token =  dbutils.secrets.get(scope = SCOPE_NAME, key = 'ls-azu-config-table-sas-key')
  config = {'storage_name': CFG_STORAGE_NAME, 'table_name': CFG_TABLE_NAME, 'token': sas_token}
  record = get_config_record(config, data)
  
  if not record:
    default_record = _get_default_config_record(partition_key, row_key)
    update_config_record(config, default_record)
    record = default_record
  return record

def load_full_dataset(source_table, options = {}):
  
  debug = options.get('debug', True)
  
  df = spark.table(source_table)
  
  if debug:
    message = "SOURCE TABLE: {0} ({1} rows) [full]".format(source_table, df.count())
    print(message)
  return df

def load_incremental_dataset(source_table, incremental_column, cutoff_value, options = {}):
  
  debug = options.get('debug', True)

  df = (
    spark.table(source_table)
    .filter("{0} > '{1}'".format(incremental_column, cutoff_value))
  )
  
  if debug:
    message = "SOURCE TABLE: {0} ({1} rows) [incremental]".format(source_table, df.count()) 
    print(message)  
  return df

def update_cutoff_value(cutoff_value, table_name, source_table = None, incremental_type = 'timestamp'):
  
  sas_token =  dbutils.secrets.get(scope = SCOPE_NAME, key = 'ls-azu-config-table-sas-key')
  config = {'storage_name': CFG_STORAGE_NAME, 'table_name': CFG_TABLE_NAME, 'token': sas_token}
  
  database_name, s_table_name = _split_table_name(table_name)
  
  partition_key = database_name.lower()
  row_key = table_name if source_table is None else "{0} [{1}]".format(s_table_name, source_table)
  row_key = row_key.lower()
  
  record = {}
  record = get_timestamp_record(table_name, source_table)
  
  data = {}
  data['PartitionKey'] = partition_key
  data['RowKey'] = row_key
  
  if cutoff_value is not None and incremental_type == "timestamp":
    data['MAX_DATETIME'] = cutoff_value.replace(' ', 'T')
    data['MAX_DATETIME@odata.type'] = 'Edm.DateTime'
  
  if cutoff_value is not None and incremental_type == "sequence":
    data['MAX_SEQUENCE'] = cutoff_value
    data['MAX_SEQUENCE@odata.type'] = 'Edm.Int32'
  
  message = 'update_cutoff: {0} | {1} | {2}'.format(partition_key, row_key, cutoff_value)
  print(message)
  
  update_config_record(config, data)
  
def update_run_datetime(run_datetime, table_name, source_table = None):
  
  sas_token =  dbutils.secrets.get(scope = SCOPE_NAME, key = 'ls-azu-config-table-sas-key')
  config = {'storage_name': CFG_STORAGE_NAME, 'table_name': CFG_TABLE_NAME, 'token': sas_token}
  
  database_name, s_table_name = _split_table_name(table_name)
  
  partition_key = database_name.lower()
  row_key = table_name if source_table is None else "{0} [{1}]".format(s_table_name, source_table)
  row_key = row_key.lower()
  
  record = {}
  record = get_timestamp_record(table_name, source_table)
  
  data = {}
  data['PartitionKey'] = partition_key
  data['RowKey'] = row_key
  data['RUN_DATETIME'] = run_datetime
  data['RUN_DATETIME@odata.type']: 'Edm.DateTime'
  
  message = 'run_datetime: {0} | {1} | {2}'.format(partition_key, row_key, run_datetime)
  print(message)
  
  update_config_record(config, data)

# COMMAND ----------

def _get_default_config_record(partition_key, row_key):
  return {
        'PartitionKey': partition_key,
        'RowKey': row_key,
        'MAX_DATETIME': '1900-01-01T00:00:00Z',
        'MAX_DATETIME@odata.type': 'Edm.DateTime',
        'RUN_DATETIME': '1900-01-01T00:00:00Z',
        'RUN_DATETIME@odata.type': 'Edm.DateTime',
        'MAX_SEQUENCE': 0,
        'MAX_SEQUENCE@odata.type': 'Edm.Int32'
    }
