# Databricks notebook source
users_list = [(1, 'Scott'), (2, 'Donald'), (3, 'Mickey'), (4, 'Elvis')]
df = spark.createDataFrame(users_list, 'user_id int, user_first_name string')

# COMMAND ----------

df.show()

# COMMAND ----------

df.collect()

# COMMAND ----------

type(df.collect())

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

help(Row)

# COMMAND ----------

r = Row("Alice", 11)

# COMMAND ----------

r

# COMMAND ----------

row2 = Row(name="Alice", age=11)

# COMMAND ----------

row2

# COMMAND ----------

row2.name

# COMMAND ----------

row2['name']

# COMMAND ----------


