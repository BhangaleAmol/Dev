# Databricks notebook source
# MAGIC %run "./02 Creating Spark Data Frame to Select and Rename Columns"

# COMMAND ----------

users_df.createOrReplaceTempView('users')

# COMMAND ----------

spark.sql("""
    SELECT id, (amount_paid + 25) AS amount_paid
    FROM users
"""). \
    show()

# COMMAND ----------

users_df. \
    selectExpr('id', '(amount_paid + 25) AS amount_paid'). \
    show()

# COMMAND ----------

# This will fail
#users_df.select('id', 'amount_paid' + 25).show()

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

lit(25)

# COMMAND ----------

# This will also fail

#users_df.select('id', 'amount_paid' + '25').show()

# COMMAND ----------

from pyspark.sql.functions import lit, col

# COMMAND ----------

'amount_paid' + lit(25.0)

# COMMAND ----------

'amount_paid' + lit('25.0')

# COMMAND ----------

# Returns null
# amount_paid is converted to string by implicitly using lit.
# Spark returns null when we perform arithmetic operations on noncompatible types
users_df.select('id', 'amount_paid' + lit(25.0), lit(50) + lit(25)).show()

# COMMAND ----------

# This works
users_df.select('id', col('amount_paid') + lit(25.0)).show()

# COMMAND ----------

# lit returns column type
lit(25.0)

# COMMAND ----------


