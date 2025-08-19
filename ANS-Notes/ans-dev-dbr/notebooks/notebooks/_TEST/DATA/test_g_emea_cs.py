# Databricks notebook source
# MAGIC %run ./bootstrap

# COMMAND ----------

database_name = 'g_emea_cs'
if database_name not in get_databases():
    dbutils.notebook.exit()
  
table_names = get_tables(database_name)
view_names = get_views(database_name)

# COMMAND ----------

class TestEmeaCs(unittest.TestCase):
  
  @parameterized.expand(table_names, skip_on_empty=True)
  def test_tables(self, table_name):    
    spark.table(table_name).sample(False, 0.1, seed=0).limit(1)
  
  @parameterized.expand(view_names, skip_on_empty=True)
  def test_view(self, view_name):
    spark.table(view_name).sample(False, 0.1, seed=0).limit(1)

# COMMAND ----------

suite = unittest.TestSuite()
suite.addTests(unittest.makeSuite(TestEmeaCs))
result = unittest.TextTestRunner(verbosity=0).run(suite)

# COMMAND ----------

failures = []
for failure in result.failures:
  failures.append(str(failure[0]))
  
for errors in result.errors:
  failures.append(str(errors[0]))

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
  "successfull": result.wasSuccessful(),
  "failures": failures
}))
