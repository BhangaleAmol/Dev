# Databricks notebook source
# MAGIC %run "./02 Creating Spark Data Frame to Select and Rename Columns"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC * Concatenate `first_name` and `last_name` to generate `full_name`

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat

# COMMAND ----------

help(concat)

# COMMAND ----------

full_name_col = concat(col('first_name'), lit(', '), col('last_name'))

# COMMAND ----------

full_name_col

# COMMAND ----------

full_name_alias = full_name_col.alias('full_name')

# COMMAND ----------

type(full_name_alias)

# COMMAND ----------

users_df.select('id', full_name_alias).show()

# COMMAND ----------

# MAGIC %md
# MAGIC * Convert data type of customer_from date to numeric type

# COMMAND ----------

users_df.select('id', 'customer_from').show()

# COMMAND ----------

from pyspark.sql.functions import date_format

# COMMAND ----------

date_format('customer_from', 'yyyyMMdd')

# COMMAND ----------

date_format('customer_from', 'yyyyMMdd').cast('int')

# COMMAND ----------

date_format('customer_from', 'yyyyMMdd').cast('int').alias('customer_from')

# COMMAND ----------

customer_from_alias = date_format('customer_from', 'yyyyMMdd').cast('int').alias('customer_from')

# COMMAND ----------

users_df.select('id', customer_from_alias).show()

# COMMAND ----------

users_df.select('id', customer_from_alias).dtypes

# COMMAND ----------


