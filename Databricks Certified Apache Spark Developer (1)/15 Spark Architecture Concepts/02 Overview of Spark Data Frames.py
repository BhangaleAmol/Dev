# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Let us make sure we understand characteristics of a Spark Data Frame.
# MAGIC * Spark Data Frame is nothing but a RDD with Structure
# MAGIC * RDD stands for Resilient Distributed Dataset. It means Reliable Distributed Collection.
# MAGIC * Spark Data Frames are immutable.
# MAGIC * As Spark Data Frames are structured, we can use structured APIs (Data Frame APIs and Spark SQL).
# MAGIC * Spark Data Frames are partitioned and they are distributed across executors while processing the data.
# MAGIC * Spark Data Frames are different from Python Pandas or R Data Frames.

# COMMAND ----------


