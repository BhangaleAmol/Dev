# Databricks notebook source
# MAGIC %run ./bootstrap

# COMMAND ----------

database_name = 's_service'
if database_name not in get_databases():
    dbutils.notebook.exit()
  
table_names = get_tables(database_name)

# COMMAND ----------

class TestService(unittest.TestCase):
  
  @parameterized.expand(table_names, skip_on_empty=True)
  def test_table_not_broken(self, table_name):
    spark.table(table_name).sample(False, 0.1, seed=0).limit(1)
    
  @parameterized.expand(table_names, skip_on_empty=True)
  def test_part_is_not_null(self, table_name):
    result = spark.sql(f"""
      SELECT COUNT(*) AS RESULT
      FROM {table_name}
      WHERE _PART IS NULL AND _SOURCE <> 'DEFAULT'
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)
    
  @parameterized.expand(table_names, skip_on_empty=True)
  def test_not_all_deleted_true(self, table_name):
    result = spark.sql(f"""
      SELECT COUNT(*) AS RESULT
      FROM {table_name}
      WHERE _DELETED IS FALSE
    """).collect()[0]['RESULT']
    self.assertTrue(result > 0)  

# COMMAND ----------

suite = unittest.TestSuite()
suite.addTests(unittest.makeSuite(TestService))
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
