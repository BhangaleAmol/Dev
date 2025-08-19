# Databricks notebook source
# MAGIC %run ./functions

# COMMAND ----------

import os
from pyspark.sql.types import *

# COMMAND ----------

hostname = "edm-mgmt-sql1.database.windows.net"
database = "edm-mgmt-db1"
table = "delta_lake_metrics_stg"
port = 1433

env_name = os.getenv('ENV_NAME')
secret_scope = f'edm-ans-{env_name}-dbr-scope'
username = dbutils.secrets.get(scope = secret_scope, key = "edm-sql-mgmt-user")
password = dbutils.secrets.get(scope = secret_scope, key = "edm-sql-mgmt-password")

# COMMAND ----------

url = f"jdbc:sqlserver://{hostname}:{port};database={database}"
connection_properties = {
  "user" : username,
  "password" : password,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

# truncate table

schema = StructType([
  StructField('version',LongType(),True),
  StructField('timestamp',TimestampType(),True),
  StructField('operation',StringType(),True),
  StructField('numOutputRows',StringType(),True),
  StructField('numTargetRowsInserted',StringType(),True),
  StructField('numTargetRowsUpdated',StringType(),True),
  StructField('numTargetFilesAdded',StringType(),True),
  StructField('numTargetFilesRemoved',StringType(),True),
  StructField('numTargetRowsDeleted',StringType(),True),
  StructField('numSourceRows',StringType(),True),
  StructField('numTargetRowsCopied',StringType(),True),
  StructField('numRemovedFiles',StringType(),True),
  StructField('numAddedFiles',StringType(),True),
  StructField('numUpdatedRows',StringType(),True),
  StructField('numCopiedRows',StringType(),True),
  StructField('executionTimeMs',StringType(),True),
  StructField('scanTimeMs',StringType(),True),
  StructField('rewriteTimeMs',StringType(),True),
  StructField('numFiles',StringType(),True),
  StructField('numOutputBytes',StringType(),True),
  StructField('table',StringType(),False),
  StructField('environment',StringType(),False)
])

result_df = spark.createDataFrame(data = [], schema = schema)

result_df.write \
  .format("jdbc") \
  .option("url", url) \
  .option("dbtable", "delta_lake_metrics_stg") \
  .option("user", username) \
  .option("password", password) \
  .save(mode = "overwrite")

# COMMAND ----------

for database_name in get_databases():
  print(database_name)

  for table_name in get_tables(database_name):
    try:
      df = get_merge_details(table_name)
      (
        df.write
        .format("jdbc")
        .option("url", url)
        .option("dbtable", table)
        .option("user", username)
        .option("password", password)
        .save(mode = "append")
      )
    except:
      pass
