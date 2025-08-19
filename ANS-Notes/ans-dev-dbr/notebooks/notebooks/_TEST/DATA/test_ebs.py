# Databricks notebook source
# MAGIC %run ./bootstrap

# COMMAND ----------

database_name = 'ebs'
if database_name not in get_databases():
    dbutils.notebook.exit()
  
table_names = get_tables(database_name)

# COMMAND ----------

class TestEbs(unittest.TestCase):
 
  def test_ebs_product_conversion_has_no_duplicates(self):
    df=spark.sql(f"""select
                    external_code,
                    internal_code,
                    party_id
                    from(
                    select distinct
                           external_code,
                          internal_code,
                          party_id
                    FROM
                      EBS.OZF_CODE_CONVERSIONS_ALL CONV
                    where
                      code_conversion_type = 'OZF_PRODUCT_CODES'
                      and ORG_ID   = 82
                      AND nvl(end_date_active,current_date) >= current_date )
                    group by
                      external_code,
                      internal_code,
                      party_id
                  having count(*) > 1""")
    self.assertEqual(df.count(), 0)


# COMMAND ----------

suite = unittest.TestSuite()
suite.addTests(unittest.makeSuite(TestEbs))
result = unittest.TextTestRunner(verbosity=1).run(suite)

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
