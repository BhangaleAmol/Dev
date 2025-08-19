# Databricks notebook source
import pyspark.sql.functions as f

# COMMAND ----------

def get_databases():  
  return [row.databaseName for row in spark.sql('SHOW DATABASES').collect()]
  
def get_tables(database_name):
  spark.sql(f'USE {database_name}')
  data = spark.sql('SHOW TABLES').select('tableName').collect()
  return [database_name + '.' + row.tableName for row in data if row.tableName[-5:] != 'delta']

def get_merge_details(table_name):
  return (
    spark.sql(f"DESCRIBE HISTORY {table_name}")
    .select(
      'version',
      'timestamp', 
      'operation', 
      'operationMetrics.numOutputRows',
      'operationMetrics.numTargetRowsInserted',
      'operationMetrics.numTargetRowsUpdated',
      'operationMetrics.numTargetFilesAdded',
      'operationMetrics.numTargetFilesRemoved',
      'operationMetrics.numTargetRowsDeleted',
      'operationMetrics.numSourceRows',
      'operationMetrics.numTargetRowsCopied',
      'operationMetrics.numRemovedFiles', 
      'operationMetrics.numAddedFiles', 
      'operationMetrics.numUpdatedRows',
      'operationMetrics.numCopiedRows',
      'operationMetrics.executionTimeMs',
      'operationMetrics.scanTimeMs',
      'operationMetrics.rewriteTimeMs',
      'operationMetrics.numFiles',
      'operationMetrics.numOutputBytes'
    )
    .withColumn('table', f.lit(table_name))
    .withColumn('environment', f.lit(env_name.upper()))
    .filter(f.col('operation').isin(['CREATE OR REPLACE TABLE AS SELECT', 'WRITE', 'MERGE', 'UPDATE']))
    .where("timestamp > current_date() - 14")
  ) 

# COMMAND ----------


