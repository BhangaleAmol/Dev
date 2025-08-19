# Databricks notebook source
users_list = [[1, 'Scott'], [2, 'Donald'], [3, 'Mickey'], [4, 'Elvis']]

# COMMAND ----------

type(users_list)

# COMMAND ----------

type(users_list[1])

# COMMAND ----------

spark.createDataFrame(users_list, 'user_id int, user_first_name string')

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

help(Row)

# COMMAND ----------

users_rows = [Row(*user) for user in users_list]

# COMMAND ----------

users_rows

# COMMAND ----------

spark.createDataFrame(users_rows, 'user_id int, user_first_name string')

# COMMAND ----------

def dummy(*args):
    print(args)
    print(len(args))

# COMMAND ----------

dummy(1)

# COMMAND ----------

dummy(1, 'Hello')

# COMMAND ----------

user_details = [1, 'Scott']

# COMMAND ----------

dummy(user_details)

# COMMAND ----------

dummy(*user_details)

# COMMAND ----------


