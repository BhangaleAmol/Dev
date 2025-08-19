# Databricks notebook source
# MAGIC %run ./func_helpers

# COMMAND ----------

from pyspark.sql.window import Window
from delta.tables import *

# COMMAND ----------

def attach_key(columns, name, map_table):
  def inner(df):
    return (
      df
      .transform(attach_surrogate_key(columns, name = "hash"))
      .alias('data')
      .join(
        spark.table(map_table).alias('map'), 
        f.col('map.hash') == f.col('data.hash'), 
        'left'
      )
      .select('data.*', 'map.key')
      .withColumn(name, f.coalesce(f.col('map.key'), f.lit('0')))
      .drop('hash', 'key')
    )
  return inner

def attach_primary_key(columns, map_table):
  def inner(df):
    update_map_table(map_table, df, columns) 
    return df.transform(attach_key(columns, "_ID", map_table))
  return inner

def create_map_table(table_name, options = {}):
    
  folder_name = (table_name.split('.')[0]).upper()
  s_table_name = table_name.split('.')[1]
  
  container_name = options.get('container_name', 'datalake')
  storage_name = options.get('storage_name', 'edmans{env}data001')
  
  endpoint_name = get_endpoint_name(container_name, storage_name)
  table_path = f"{endpoint_name}/datalake/{folder_name}/{s_table_name}.dlt"
  
  spark.sql("CREATE DATABASE IF NOT EXISTS edm")
  
  spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
      hash STRING NOT NULL,
      id BIGINT NOT NULL,
      source STRING NOT NULL,
      key STRING NOT NULL,
      nk STRING
    )
    USING DELTA
    PARTITIONED BY (source)
    LOCATION '{table_path}'
  """)

  spark.sql(f"""
    INSERT INTO {table_name}
    VALUES ('0', 0, 'default', '0', '0')
  """)

def update_all_foreign_keys(source_table, columns, name, map_table):  
  
  df = (
    spark.table(source_table)
    .filter(f.col('_SOURCE') != 'DEFAULT')
    .filter("_ID <> '0'")  
  )
  
  df_with_key = df.transform(attach_key(columns, name, map_table))
  
  deltaTable = DeltaTable.forName(spark, source_table)
  deltaTable.alias("t") \
  .merge(df_with_key.alias("s"), "t._ID = s._ID AND t._DELETED = s._DELETED") \
  .whenMatchedUpdate(set = { name : f"s.{name}" }) \
  .execute() 
  
def update_all_foreign_keys_to_hash(source_table, columns, name):  
  
  df_with_hash = (
    spark.table(source_table)
    .filter(f.col('_SOURCE') != 'DEFAULT')
    .transform(attach_surrogate_key(columns, name = "hash"))
  )
  
  deltaTable = DeltaTable.forName(spark, source_table)
  deltaTable.alias("t") \
  .merge(df_with_hash.alias("s"), "t._ID = s._ID AND t._DELETED = s._DELETED") \
  .whenMatchedUpdate(set = { name : "s.hash" }) \
  .execute() 
  
def update_all_primary_keys(source_table, columns, map_table):  
  
  df = (
    spark.table(source_table)
    .filter("_SOURCE <> 'DEFAULT'")
  )
  
  df_with_key = df.transform(attach_key(columns, '_key', map_table))
  
  deltaTable = DeltaTable.forName(spark, source_table)
  deltaTable.alias("t") \
  .merge(df_with_key.alias("s"), "t._ID = s._ID AND t._DELETED = s._DELETED") \
  .whenMatchedUpdate(set = { '_ID' : "_key" }) \
  .execute()   

def update_all_primary_keys_to_hash(source_table, columns):  
  
  df = (
    spark.table(source_table)
    .filter(f.col('_SOURCE') != 'DEFAULT')
    .transform(attach_surrogate_key(columns, name = "hash"))
  )  
  
  deltaTable = DeltaTable.forName(spark, source_table)
  deltaTable.alias("t") \
  .merge(df.alias("s"), "t._ID = s._ID AND t._DELETED = s._DELETED") \
  .whenMatchedUpdate(set = { '_ID' : "s.hash" }) \
  .execute()    
  
def update_foreign_key(source_table, columns, name, map_table, source_name = None):
  
  df = (
    spark.table(source_table)
    .filter(f.col('_SOURCE') != 'DEFAULT')
    .filter(f.col(name) == '0')
    .filter("_ID <> '0'")
  )
  
  if source_name is not None:
    src_df = df.filter(f.col('_SOURCE') == source_name)
  else:
    src_df = df
  
  df_with_key = (
    src_df
    .transform(attach_key(columns, name, map_table))
    .filter(f.col(name) != '0')
  )
  
  print(f"{source_table}, {name}")
  
  join_str = "t._ID = s._ID AND t._DELETED = s._DELETED"
  if source_name is not None:
    join_str = f"{join_str} AND t._SOURCE = '{source_name}'"
  
  deltaTable = DeltaTable.forName(spark, source_table)
  deltaTable.alias("t") \
  .merge(df_with_key.alias("s"), join_str) \
  .whenMatchedUpdateAll() \
  .execute()  
  
def update_map_table(map_table, df, columns):
  
  if isinstance(columns, str):
    columns_list = [c.strip() for c in columns.split(',')]
  else:
    columns_list = columns
  
  hash_df = (
    df
    .transform(attach_surrogate_key(columns, name = "hash"))
    .filter(f.col('_SOURCE') != 'DEFAULT')
    .select(
      'hash', 
      f.lower(f.col('_SOURCE')).alias('source'), 
      f.concat_ws('|', *columns_list).alias('nk')
    )
    .distinct()
  ).alias('hash')
  
  map_df = spark.table(map_table).alias('map')
  
  max_df = (
     map_df
    .groupBy('source')
    .agg(f.coalesce(f.max('id'), f.lit(0)).alias('max_id'))
  ).alias('max')
  
  new_map_df = (
     hash_df
    .join(map_df, f.col('map.hash') == f.col('hash.hash'), 'leftanti')
    .join(max_df, f.col('hash.source') == f.col('max.source'), 'left')
    .withColumn(
      'id', 
      f.coalesce(f.col('max.max_id'), f.lit(0)) + f.row_number().over(
        Window.partitionBy('hash.source').orderBy('hash.hash')
      )
    )
    .withColumn('key', f.concat(f.col('hash.source'), f.lit('-'), f.col('id')))
    .select('hash.hash', 'hash.source', 'id', 'key', 'nk')
  )
   
  message = f"{new_map_df.count()} records added to {map_table} map table"
  print(message)
  
  deltaTable = DeltaTable.forName(spark, map_table)
  deltaTable.alias("t") \
  .merge(new_map_df.alias("s"), "t.hash = s.hash") \
  .whenNotMatchedInsertAll() \
  .execute()
