# Databricks notebook source
# MAGIC %run ./bootstrap

# COMMAND ----------

database_name = 'g_fin_qv'
if database_name not in get_databases():
    dbutils.notebook.exit()
  
table_names = get_tables(database_name)
view_names = get_views(database_name)

# COMMAND ----------

class TestFinQv(unittest.TestCase):
  
  @parameterized.expand(table_names, skip_on_empty=True)
  def test_tables(self, table_name):    
    spark.table(table_name).sample(False, 0.1, seed=0).limit(1)
  
  @parameterized.expand(view_names, skip_on_empty=True)
  def test_view(self, view_name):
    spark.table(view_name).sample(False, 0.1, seed=0).limit(1)
    
    
  def test_org_assignments_non_null_keys(self):
    df = spark.table("g_fin_qv.wc_intransit_extract_fs") \
      .select('ITEM_NUMBER', 'INVENTORY_ORG') \
      .filter("ITEM_NUMBER IS NULL OR INVENTORY_ORG IS NULL ")
    self.assertEqual(df.count(), 0)
    
        
  def test_org_assignments_non_null_keys(self):
    df = spark.table("g_fin_qv.wc_qv_intransit_details_f") \
      .select('ITEM_NUMBER', 'ORG_CHANNEL') \
      .filter("ITEM_NUMBER IS NULL OR ORG_CHANNEL IS NULL ")
    self.assertEqual(df.count(), 0)
    
        
  def test_org_assignments_non_null_keys(self):
    df = spark.table("g_fin_qv.wc_purch_price_variance_f") \
      .select('INVENTORY_ITEM_ID', 'ORGANIZATION_ID') \
      .filter("INVENTORY_ITEM_ID IS NULL OR ORGANIZATION_ID IS NULL ")
    self.assertEqual(df.count(), 0)
        
    

# COMMAND ----------

suite = unittest.TestSuite()
suite.addTests(unittest.makeSuite(TestFinQv))
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
