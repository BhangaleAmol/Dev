# Databricks notebook source
# MAGIC %run "./02 Creating Spark Data Frame to Select and Rename Columns"

# COMMAND ----------

users_df. \
    select('id', 'first_name', 'last_name'). \
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC * Concat `first_name` and `last_name`. Provide an alias to the derived result as `full_name`

# COMMAND ----------

from pyspark.sql.functions import concat, lit

# COMMAND ----------

# Equivalent logic using select
users_df. \
    select(
        'id', 'first_name', 'last_name',
        concat('first_name', lit(', '), 'last_name').alias('full_name')
    ). \
    show()

# COMMAND ----------

users_df. \
    select('id', 'first_name', 'last_name'). \
    withColumn('full_name', concat('first_name', lit(', '), 'last_name')). \
    show()

# COMMAND ----------

help(users_df.withColumn)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

users_df. \
    select('id', 'first_name', 'last_name'). \
    withColumn('fn', users_df['first_name']). \
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC * Add another column by name `course_count` where it contain number of courses the user is enrolled for.

# COMMAND ----------

users_df.select('id', 'courses'). \
    show()

# COMMAND ----------

users_df.select('id', 'courses'). \
    dtypes

# COMMAND ----------

from pyspark.sql.functions import size

# COMMAND ----------

users_df.select('id', 'courses'). \
    withColumn('course_count', size('courses')). \
    show()

# COMMAND ----------


