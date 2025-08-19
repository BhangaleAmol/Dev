# Databricks notebook source
# MAGIC %run "./02 Creating Spark Data Frame to Select and Rename Columns"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC * Rename `id` to `user_id`
# MAGIC * Rename `first_name` to `user_first_name`
# MAGIC * Rename `last_name` to `user_last_name`
# MAGIC * Also add new column by name `user_full_name` which is derived by concatenating `first_name` and `last_name` with `, ` in between.

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit

# COMMAND ----------

user_id = col('id')

# COMMAND ----------

help(user_id.alias)

# COMMAND ----------

# Using select
users_df. \
    select(
        col('id').alias('user_id'),
        col('first_name').alias('user_first_name'),
        col('last_name').alias('user_last_name'),
        concat(col('first_name'), lit(', '), col('last_name')).alias('user_full_name')
    ). \
    show()

# COMMAND ----------

users_df. \
    select(
        users_df['id'].alias('user_id'),
        users_df['first_name'].alias('user_first_name'),
        users_df['last_name'].alias('user_last_name'),
        concat(users_df['first_name'], lit(', '), users_df['last_name']).alias('user_full_name')
    ). \
    show()

# COMMAND ----------

# Using withColumn and alias (first select and then withColumn)
users_df. \
    select(
        users_df['id'].alias('user_id'),
        users_df['first_name'].alias('user_first_name'),
        users_df['last_name'].alias('user_last_name')
    ). \
    withColumn('user_full_name', concat(col('user_first_name'), lit(', '), col('user_last_name'))). \
    show()

# COMMAND ----------

# Using withColumn and alias (first withColumn and then select)
users_df. \
    withColumn('user_full_name', concat(col('first_name'), lit(', '), col('last_name'))). \
    select(
        users_df['id'].alias('user_id'),
        users_df['first_name'].alias('user_first_name'),
        users_df['last_name'].alias('user_last_name'),
        'user_full_name'
    ). \
    show()

# COMMAND ----------

users_df. \
    withColumn('user_full_name', concat(users_df['first_name'], lit(', '), users_df['last_name'])). \
    select(
        users_df['id'].alias('user_id'),
        users_df['first_name'].alias('user_first_name'),
        users_df['last_name'].alias('user_last_name'),
        'user_full_name'
    ). \
    show()

# COMMAND ----------


