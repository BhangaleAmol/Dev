# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Here are the steps we need to follow to develop and use Spark User Defined Functions.
# MAGIC * Develop required logic using Python as programming language.
# MAGIC * Register the function using `spark.udf.register`. Also assign it to a variable.
# MAGIC * Variable can be used as part of Data Frame APIs such as `select`, `filter`, etc.
# MAGIC * When we register, we register with a name. That name can be used as part of `selectExpr` or as part of Spark SQL queries using `spark.sql`.

# COMMAND ----------

help(spark.udf.register)
