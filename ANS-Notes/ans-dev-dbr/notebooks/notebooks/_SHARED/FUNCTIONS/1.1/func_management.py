# Databricks notebook source
def register_hive_tables_from_folder(database_name, folder_path):
  spark.sql("CREATE DATABASE IF NOT EXISTS {0}".format(database_name))
  spark.sql("USE {0}".format(database_name))

  files = list(dbutils.fs.ls(folder_path))
  for file in files:
    tableName = file.name.split('.')[0]
    filePath = file.path
    query = "CREATE TABLE IF NOT EXISTS {0} USING DELTA LOCATION '{1}'".format(tableName, filePath)
    print(query)
    spark.sql(query)
    spark.sql("ALTER TABLE {0} SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)".format(tableName))
    spark.sql("ANALYZE TABLE {0} COMPUTE STATISTICS NOSCAN".format(tableName))

def show_hive_table_properties(database_name, table_name = None):
  
  spark.sql("USE {0}".format(database_name))
  
  schema = StructType([
    StructField('key', StringType(), True),
    StructField('value', StringType(), True),
    StructField('tableName', StringType(), True),
    StructField('location', StringType(), True),
    StructField('partitions', StringType(), True),
    StructField('rows', StringType(), True),
  ])
  
  result = spark.createDataFrame([], schema)
  
  if table_name is None:
    data = spark.sql("SHOW TABLES").select('tableName').collect()
    table_name = [row.tableName for row in data]
  
  for name in table_name:
    
    location = get_hive_table_location(name)
    
    try:
      partitions = spark.sql("SHOW PARTITIONS {0}".format(name))
      partition_cols = ','.join([str(column) for column in partitions.columns])
    except:
      partition_cols = 'none'
        
    rows = spark.table(name).count()
        
    query = "SHOW TBLPROPERTIES {0}".format(name)
    df = (
      spark.sql(query)
      .withColumn('tableName', f.lit(name))
      .withColumn('location', f.lit(location))
      .withColumn('partitions', f.lit(partition_cols))
      .withColumn('rows', f.lit(rows))
    )
    result = result.union(df)   
    
  result_f = (
    result
    .groupby('tableName', 'location', 'partitions', 'rows')
    .pivot("key")
    .agg(f.first("value"))
    .orderBy('tableName', ascending = True)
  )
  display(result_f)  
    
def shrink_hive_table(database_name, table_name):
  spark.sql("USE {0}".format(database_name))  
  print(table_name)
  query = "VACUUM {0}".format(table_name)
  print(query)
  spark.sql(query)
  query = "OPTIMIZE {0}".format(table_name)
  print(query)
  spark.sql(query)
  query = "ALTER TABLE {0} SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)".format(table_name)
  print(query)
  spark.sql(query) 
  query = "ANALYZE TABLE {0} COMPUTE STATISTICS NOSCAN".format(table_name)
  print(query)
  spark.sql(query)
  
def shrink_hive_tables(database_name):
  spark.sql("USE {0}".format(database_name))  
  data = spark.sql("SHOW TABLES").select('tableName').collect()
  for row in data:
    shrink_hive_table(database_name, row.tableName)  

# COMMAND ----------


