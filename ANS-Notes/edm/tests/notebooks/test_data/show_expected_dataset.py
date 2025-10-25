# Databricks notebook source
# MAGIC %pip install pytest==7.2.0
# MAGIC %pip install pytest-mock==3.10.0

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# if result is correct, save as _expected.csv

from tests.notebooks.s_core.test_account_kgd import get_datasets
from databricks.functions.s_core.account_kgd import transform_data

datasets = get_datasets()
actual_df = transform_data(datasets)
actual_df.display()

# COMMAND ----------

from databricks.functions.shared.transformations import replace_empty_string_with_null
from databricks.functions.shared.tests import *

expected_df = (
    read_test_data('/s_core/account_ebs/_expected.csv')
    .withColumn('_DATE', f.current_date())
)
expected_df.display()
