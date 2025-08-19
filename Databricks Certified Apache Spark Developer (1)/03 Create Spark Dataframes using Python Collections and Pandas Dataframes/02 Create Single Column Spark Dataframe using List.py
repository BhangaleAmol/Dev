# Databricks notebook source
ages_list = [21, 23, 18, 41, 32]

# COMMAND ----------

type(ages_list)

# COMMAND ----------

spark

# COMMAND ----------

help(spark.createDataFrame)

# COMMAND ----------

#spark.createDataFrame(ages_list)

# COMMAND ----------

spark.createDataFrame(ages_list, 'int')

# COMMAND ----------

from pyspark.sql.types import IntegerType

# COMMAND ----------

spark.createDataFrame(ages_list, IntegerType())

# COMMAND ----------

names_list = ['Scott', 'Donald', 'Mickey']

# COMMAND ----------

spark.createDataFrame(names_list, 'string')

# COMMAND ----------

from pyspark.sql.types import StringType

# COMMAND ----------

spark.createDataFrame(names_list, StringType())

# COMMAND ----------


