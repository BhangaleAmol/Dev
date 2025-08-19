# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC * Spark provides robust set of pre-defined functions as part of `pyspark.sql.functions`.
# MAGIC * However, they might not fulfill all our requirements.
# MAGIC * At times, we might have to develop custom UDFs for these scenarios.
# MAGIC   * No function available for our requirement while applying row level transformations.
# MAGIC   * Also, we might have to use multiple functions due to which readability of the code is compromised.

# COMMAND ----------


