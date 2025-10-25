# Databricks notebook source
# MAGIC %pip install pytest==7.2.0
# MAGIC %pip install pytest-mock==3.10.0
# MAGIC %pip install pytest-xdist==3.2.1

# COMMAND ----------

from databricks.sdk.runtime import dbutils
dbutils.library.restartPython()

# COMMAND ----------

import os
os.environ["PYTHONDONTWRITEBYTECODE"] = "1"

# COMMAND ----------

import pytest
import sys
import re

# COMMAND ----------

import databricks.functions.shared.bootstrap as boot
boot.bootstrap()

# COMMAND ----------

notebook_path = dbutils.notebook.entry_point.getDbutils(
).notebook().getContext().notebookPath().get()
repo_root = os.path.dirname(os.path.dirname(notebook_path))
os.chdir(f'/Workspace/{repo_root}')

# COMMAND ----------

# database_name
default_database = 'test_' + \
    re.sub(r'[^A-Za-z0-9_]+', '_', repo_root.split('/')[-1:][0])
dbutils.widgets.text("database_name", default_database)
database_name = dbutils.widgets.get("database_name")

# details
dbutils.widgets.dropdown("details", "false", ['true', 'false'])
details = dbutils.widgets.get("details")

# environment
dbutils.widgets.dropdown("environment", "default", [
                         'default', 'dev', 'test', 'prod'])
environment = dbutils.widgets.get("environment")

# folder_path
dbutils.widgets.text("folder_path", "./tests")
folder_path = dbutils.widgets.get("folder_path")

# ignore_path
dbutils.widgets.text("ignore_path", "")
ignore_path = dbutils.widgets.get("ignore_path")
ignore_path_params = []
if ignore_path:
    ignore_path_params = (
        '--ignore,' + ',--ignore,'.join(ignore_path.split(','))).split(',')

# markers
# dbutils.widgets.text("markers", "not test and not prod")
# markers = dbutils.widgets.get("markers")

# report_path
dbutils.widgets.text("report_path", "./tests/report.xml")
report_path = dbutils.widgets.get("report_path")

# COMMAND ----------

# MAGIC %pwd

# COMMAND ----------

params = [
    folder_path,
    "-p", "no:cacheprovider",
    "--database_name", database_name,
    "--environment", environment,
    "--junitxml", report_path,
    "-W ignore::DeprecationWarning",
    # "-m", markers,
    "-n", "4", "--dist=loadfile"]

params.extend(ignore_path_params)

if details == 'true':
    params.extend(["--details", str(details), "-v", "-s"])

print(params)

sys.dont_write_bytecode = True
retcode = pytest.main(params)
assert retcode == 0, 'The pytest invocation failed'

# COMMAND ----------

# test_path = 'tests/functions/test_shared_catalog.py::test_insert_into_table'
