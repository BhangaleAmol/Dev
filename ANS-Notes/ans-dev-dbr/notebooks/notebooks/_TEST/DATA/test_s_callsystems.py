# Databricks notebook source
# MAGIC %run ./bootstrap

# COMMAND ----------

database_name = 's_callsystems'
if database_name not in get_databases():
    dbutils.notebook.exit()
    
table_names = get_tables(database_name)

# COMMAND ----------

class TestCallSystems(unittest.TestCase):
  
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
    
  def test_customer_session_no_missing_dim_records(self):
    result = spark.sql("""
      SELECT COUNT(*) AS RESULT 
      FROM s_callsystems.fact_customer_session f
      LEFT JOIN s_callsystems.dim_agent a ON f.AGENT_ID = a.AGENT_ID
      LEFT JOIN s_callsystems.dim_entrypoint e ON f.ENTRYPOINT_ID = e.ENTRYPOINT_ID
      LEFT JOIN s_callsystems.dim_queue q ON f.QUEUE_ID = q.QUEUE_ID
      LEFT JOIN s_callsystems.dim_site s ON f.SITE_ID = s.SITE_ID
      LEFT JOIN s_callsystems.dim_team t ON f.TEAM_ID = t.TEAM_ID
      WHERE 
        a.AGENT_ID IS NULL
        OR e.ENTRYPOINT_ID IS NULL
        OR q.QUEUE_ID IS NULL
        OR s.SITE_ID IS NULL
        OR t.TEAM_ID IS NULL
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)
  
  def test_customer_activity_no_missing_dim_records(self):
    result = spark.sql("""
      SELECT COUNT(*) AS RESULT 
      FROM s_callsystems.fact_customer_activity f
      LEFT JOIN s_callsystems.dim_agent a ON f.AGENT_ID = a.AGENT_ID
      LEFT JOIN s_callsystems.dim_queue q ON f.QUEUE_ID = q.QUEUE_ID
      LEFT JOIN s_callsystems.dim_site s ON f.SITE_ID = s.SITE_ID
      LEFT JOIN s_callsystems.dim_team t ON f.TEAM_ID = t.TEAM_ID
      WHERE 
        a.AGENT_ID IS NULL
        OR q.QUEUE_ID IS NULL
        OR s.SITE_ID IS NULL
        OR t.TEAM_ID IS NULL
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)
    
  def test_agent_activity_no_missing_dim_records(self):
    result = spark.sql("""
      SELECT COUNT(*) AS RESULT 
      FROM s_callsystems.fact_agent_activity f
      LEFT JOIN s_callsystems.dim_agent a ON f.AGENT_ID = a.AGENT_ID
      LEFT JOIN s_callsystems.dim_queue q ON f.QUEUE_ID = q.QUEUE_ID
      LEFT JOIN s_callsystems.dim_site s ON f.SITE_ID = s.SITE_ID
      LEFT JOIN s_callsystems.dim_team t ON f.TEAM_ID = t.TEAM_ID
      WHERE 
        a.AGENT_ID IS NULL
        OR q.QUEUE_ID IS NULL
        OR s.SITE_ID IS NULL
        OR t.TEAM_ID IS NULL
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)
    
  def test_queue_name_has_one_case_version(self):
    result = spark.sql("""
      WITH CTE AS(
        SELECT
        LOWER(QUEUE_NAME),
        COUNT(*)
        FROM s_callsystems.DIM_QUEUE
        WHERE _DELETED IS FALSE
        GROUP BY LOWER(QUEUE_NAME)
        HAVING COUNT(*) > 1
      )
      SELECT COUNT(*) AS RESULT FROM CTE
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)
  
  def test_entrypoint_name_has_one_case_version(self):
    result = spark.sql("""
      WITH CTE AS(
        SELECT
        LOWER(ENTRYPOINT_NAME),
        COUNT(*)
        FROM s_callsystems.DIM_ENTRYPOINT
        WHERE _DELETED IS FALSE
        GROUP BY LOWER(ENTRYPOINT_NAME)
        HAVING COUNT(*) > 1
      )
      SELECT COUNT(*) AS RESULT FROM CTE
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)
    
  def test_team_name_has_one_case_version(self):
    result = spark.sql("""
      WITH CTE AS(
        SELECT
        LOWER(TEAM_NAME),
        COUNT(*)
        FROM s_callsystems.DIM_TEAM
        WHERE _DELETED IS FALSE
        GROUP BY LOWER(TEAM_NAME)
        HAVING COUNT(*) > 1
      )
      SELECT COUNT(*) AS RESULT FROM CTE
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)
    
  def test_no_new_cowansville_endpoints_added(self):
    result = spark.sql("""
      WITH CTE AS(
      SELECT
        ENTRYPOINT_NAME
      FROM s_callsystems.DIM_ENTRYPOINT
      WHERE _DELETED IS FALSE
      AND ENTRYPOINT_NAME NOT IN (
        'EP_NA_CS_Cowansville_General_LangMenu',
        'EP_NA_CS_Cowansville_Partners_Eng',
        'EP_NA_CS_Cowansville_General_Fr',
        'EP_NA_CS_Cowansville_General_Eng',
        'EP_NA_CS_Cowansville_Partners_LangMenu',
        'EP_NA_CS_Cowansville_Partners_Fr'
      )
      AND ENTRYPOINT_NAME LIKE 'EP_NA_CS_Cowansville%'
      GROUP BY ENTRYPOINT_NAME
      HAVING COUNT(*) > 1
    )
    SELECT COUNT(*) AS RESULT FROM CTE
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)

# COMMAND ----------

suite = unittest.TestSuite()
suite.addTests(unittest.makeSuite(TestCallSystems))
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
