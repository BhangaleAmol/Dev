# Databricks notebook source
import getpass
username = getpass.getuser()

# COMMAND ----------

spark.read.csv('dbfs:/databricks-datasets/asa/airlines', header=True).count()

# COMMAND ----------

spark.read.csv(f'/user/{username}/asa/airlines', header=True).count()

# COMMAND ----------

spark.read.csv('dbfs:/databricks-datasets/asa/airlines', header=True). \
    filter('Year = 2004'). \
    count()

# COMMAND ----------

spark.read.csv(f'/user/{username}/asa/airlines/Year=2004', header=True).count()

# COMMAND ----------

dbutils.fs.ls(f'/user/{username}/asa/airlines/')

# COMMAND ----------

spark.read.csv(f'/user/{username}/asa/airlines', header=True).filter('Year = 2004').count()

# COMMAND ----------

airlines_df = spark.read.csv(f'/user/{username}/asa/airlines', header=True)

# COMMAND ----------

airlines_df. \
    createOrReplaceTempView('airlines')

# COMMAND ----------

spark.sql('SHOW tables').show()

# COMMAND ----------

spark.sql('''
    SELECT count(*)
    FROM airlines
    WHERE year = 2004
''').show()

# COMMAND ----------


