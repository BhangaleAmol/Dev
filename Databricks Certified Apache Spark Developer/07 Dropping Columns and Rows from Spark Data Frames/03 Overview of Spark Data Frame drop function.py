# Databricks notebook source
# MAGIC %run "./02 Creating Spark Data Frame for Dropping Columns"

# COMMAND ----------

users_df.show()

# COMMAND ----------

users_df.select('*').show()

# COMMAND ----------

help(users_df.drop)

# COMMAND ----------


