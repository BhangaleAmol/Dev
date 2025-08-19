# Databricks notebook source
# MAGIC %run ./bootstrap

# COMMAND ----------

database_name = 'pdh'
if database_name not in get_databases():
    dbutils.notebook.exit()
  
table_names = get_tables(database_name)

# COMMAND ----------

class TestPdh(unittest.TestCase):
  
  @parameterized.expand(table_names, skip_on_empty=True)
  def test_table_not_broken(self, table_name):
    spark.table(table_name).sample(False, 0.1, seed=0).limit(1)    
    
  @parameterized.expand(table_names, skip_on_empty=True)
  def test_not_all_deleted_true(self, table_name):
    result = spark.sql(f"""
      SELECT COUNT(*) AS RESULT
      FROM {table_name}
      WHERE _DELETED IS FALSE
    """).collect()[0]['RESULT']
    self.assertTrue(result > 0)
  
  # org_assignments
  def test_org_assignments_pk_is_unique(self):
    df = spark.table("pdh.org_assignments") \
      .select('ITEM_NAME', 'ORGANIZATION_NAME', 'ORG_WAREHOUSE')
    df.cache()
    self.assertEqual(df.count(), df.distinct().count())
  
  def test_org_assignments_non_null_keys(self):
    df = spark.table("pdh.org_assignments") \
      .select('ITEM_NAME', 'ORGANIZATION_NAME', 'ORG_WAREHOUSE') \
      .filter("ITEM_NAME IS NULL OR ORGANIZATION_NAME IS NULL OR ORG_WAREHOUSE IS NULL")
    self.assertEqual(df.count(), 0)
  
  # customs_codes
  def test_customs_codes_pk_is_unique(self):
    df = spark.table("pdh.customs_codes") \
      .select('ITEM_NAME', 'ORGANIZATION_NAME', 'SOS')
    df.cache()
    self.assertEqual(df.count(), df.distinct().count())
  
  def test_customs_codes_non_null_keys(self):
    df = spark.table("pdh.customs_codes") \
      .select('ITEM_NAME', 'ORGANIZATION_NAME', 'SOS') \
      .filter("ITEM_NAME IS NULL OR ORGANIZATION_NAME IS NULL OR SOS IS NULL")
    self.assertEqual(df.count(), 0)
    
  # master_records
  def test_master_records_pk_is_unique(self):
    df = spark.table("pdh.master_records") \
      .select('ITEM')
    df.cache()
    self.assertEqual(df.count(), df.distinct().count())
  
  def test_master_records_non_null_keys(self):
    df = spark.table("pdh.master_records") \
      .select('ITEM') \
      .filter("ITEM IS NULL")
    self.assertEqual(df.count(), 0)
    
  # regional
  def test_regional_pk_is_unique(self):
    df = spark.table("pdh.regional") \
      .select('ITEM')
    df.cache()
    self.assertEqual(df.count(), df.distinct().count())
  
  def test_regional_non_null_keys(self):
    df = spark.table("pdh.regional") \
      .select('ITEM') \
      .filter("ITEM IS NULL")
    self.assertEqual(df.count(), 0)
  
  # source_of_supply
  def test_source_of_supply_pk_is_unique(self):
    df = spark.table("pdh.source_of_supply") \
      .select('ITEM', 'SOS_CODE')
    df.cache()
    self.assertEqual(df.count(), df.distinct().count())
  
  def test_source_of_supply_non_null_keys(self):
    df = spark.table("pdh.source_of_supply") \
      .select('ITEM', 'SOS_CODE') \
      .filter("ITEM IS NULL OR SOS_CODE IS NULL")
    self.assertEqual(df.count(), 0)
  
  def test_master_record_item_in_source_of_supply(self):
    df=spark.sql(f"""
      select * 
      from pdh.master_records 
      where 
        item not in (select item from pdh.source_of_supply)""")
    self.assertEqual(df.count(), 0)
    
  def test_source_of_supply_item_has_one_and_only_one_primary_sos(self):
    df=spark.sql(f"""
      select item,count(*) 
      from pdh.source_of_supply 
      where primary_origin_flag = 'Y'
      group by item having count(*)>1""")
    self.assertEqual(df.count(), 0)
  
  def test_master_record_item_has_at_least_one_primary_origin(self):
    df=spark.sql(f"""
      select * 
      from pdh.master_records 
      where item not in (select item from pdh.source_of_supply where primary_origin_flag = 'Y')""")
    self.assertEqual(df.count(), 0)
    
  def test_source_of_supply_item_has_not_more_than_one_sos(self):
    df=spark.sql(f"""select item 
                      from (select distinct item, sos_code from pdh.source_of_supply
                            where primary_origin_flag = 'Y')
                      group by item having count(*) >1""")
    self.assertEqual(df.count(), 0)

# COMMAND ----------

suite = unittest.TestSuite()
suite.addTests(unittest.makeSuite(TestPdh))
result = unittest.TextTestRunner(verbosity=1).run(suite)

# COMMAND ----------

failures = []
for failure in result.failures:
  failures.append(str(failure[0]))
  
for errors in result.errors:
  failures.append(str(errors[0]))
  
print('\n'.join(failures))

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
  "successfull": result.wasSuccessful(),
  "failures": failures
}))

# COMMAND ----------


