# Databricks notebook source
# MAGIC %run "./02 Creating Spark Data Frame to Select and Rename Columns"

# COMMAND ----------

users_df['id']

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

col('id')

# COMMAND ----------

type(users_df['id'])

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

users_df.select('id', col('first_name'), 'last_name').show()

# COMMAND ----------

users_df.select(users_df['id'], col('first_name'), 'last_name').show()

# COMMAND ----------

# This does not work as there is no object by name u in this session.
#users_df.alias('u').select(u['id'], col('first_name'), 'last_name').show()

# COMMAND ----------

# This shall work

users_df.alias('u').select('u.id', col('first_name'), 'last_name').show()

# COMMAND ----------

# This does not work as selectExpr can only take column names or SQL style expressions on column names
users_df.selectExpr(col('id'), 'first_name', 'last_name').show()

# COMMAND ----------

from pyspark.sql.functions import concat, lit, col

# COMMAND ----------

users_df. \
    select(
        'id', 'first_name', 'last_name', 
        concat(users_df['first_name'], lit(', '), col('last_name')).alias('full_name')
    ). \
    show()

# COMMAND ----------

# Using selectExpr to use Spark SQL Functions
users_df.alias('u').selectExpr('id', 'first_name', 'last_name', "concat(u.first_name, ', ', u.last_name) AS full_name").show()

# COMMAND ----------

users_df.createOrReplaceTempView('users')

# COMMAND ----------

spark.sql("""
    SELECT id, first_name, last_name,
        concat(u.first_name, ', ', u.last_name) AS full_name
    FROM users AS u
"""). \
    show()

# COMMAND ----------


