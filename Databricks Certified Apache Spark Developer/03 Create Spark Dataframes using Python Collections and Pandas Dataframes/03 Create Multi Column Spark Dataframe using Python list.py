# Databricks notebook source
ages_list = [(21, ), (23, ), (41, ), (32, )]

# COMMAND ----------

type(ages_list)

# COMMAND ----------

type(ages_list[2])

# COMMAND ----------

spark.createDataFrame(ages_list, 'age int')

# COMMAND ----------

users_list = [(1, 'Scott'), (2, 'Donald'), (3, 'Mickey'), (4, 'Elvis')]

# COMMAND ----------

spark.createDataFrame(users_list)

# COMMAND ----------

spark.createDataFrame(users_list, 'user_id int, user_first_name string')
