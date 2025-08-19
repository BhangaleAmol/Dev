# Databricks notebook source
# MAGIC %run ./bootstrap

# COMMAND ----------

database_name = 's_manufacturing'
if database_name not in get_databases():
    dbutils.notebook.exit()
    
table_names = get_tables(database_name)

# COMMAND ----------

class TestManufacturing(unittest.TestCase):
  
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
    
  def test_bkk_and_mlk_process_not_dipping(self):
    result = spark.sql("""
      SELECT COUNT(*) AS RESULT
      FROM s_manufacturing.production_data_agg
      WHERE
        PLANT IN ('BKK','MLK')
        AND PROCESS != 'Dipping'
        AND _DELETED IS FALSE
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)
    
  def test_axl_not_folding(self):
    result = spark.sql("""
      SELECT COUNT(*) AS RESULT
      FROM s_manufacturing.production_data_agg
      WHERE
        PLANT='AXL'
        AND PROCESS != 'Folding'
        AND _DELETED IS FALSE
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)
    
  def test_actual_production_greater_than_1_and_5(self):
    result = spark.sql("""
      WITH CTE AS(
        SELECT 
          DATE,
          PLANT,
          MACHINE
        FROM s_manufacturing.production_data_agg
        WHERE _DELETED IS FALSE
        GROUP BY
          DATE,
          PLANT,
          MACHINE
        HAVING SUM(ACTUAL_PRODUCTION)/SUM(TARGET_PRODUCTION) >= 1.5
      )
      SELECT COUNT(*) AS RESULT FROM CTE
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)
    
    
  def test_actual_production_lower_than_0(self):
    result = spark.sql("""
      WITH CTE AS(
        SELECT 
          DATE,
          PLANT,
          MACHINE
        FROM s_manufacturing.production_data_agg
        WHERE _DELETED IS FALSE
        GROUP BY
          DATE,
          PLANT,
          MACHINE
        HAVING SUM(ACTUAL_PRODUCTION)/SUM(TARGET_PRODUCTION) < 0
      )
      SELECT COUNT(*) AS RESULT FROM CTE
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)
 
  def test_empty_reworks(self):
    result = spark.sql("""
      WITH CTE AS(
        SELECT
          COUNT(*) AS RESULT
        FROM s_manufacturing.production_data_agg
        WHERE TOTAL_REWORKS IS NULL AND _DELETED IS FALSE
        HAVING COUNT(*) > 0
      )
      SELECT COUNT(*) AS RESULT FROM CTE
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)
    
  def test_mlk_bkk_empty_total_downtime(self):
    result = spark.sql("""
      WITH CTE AS(
        SELECT
          COUNT(*) AS RESULT
        FROM s_manufacturing.production_data_agg
        WHERE TOTAL_DOWNTIME_MIN IS NULL AND _DELETED IS FALSE
        AND PLANT IN ('BKK','MLK')
        HAVING COUNT(*) > 0
      )
      SELECT COUNT(*) AS RESULT FROM CTE
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)    
    

  def test_actual_production_lower_than_rejects_and_reworks(self):
    result = spark.sql("""
      WITH CTE AS (
        SELECT 
          YEAR(DATE)*100+MONTH(DATE) AS YEARMONTH,
          MACHINE,
          PLANT
        FROM s_manufacturing.production_data_agg
        WHERE _DELETED IS FALSE
        GROUP BY
          YEAR(DATE)*100+MONTH(DATE),
          MACHINE,
          PLANT
        HAVING 
        CASE 
          WHEN PLANT='MLK' 
          THEN SUM(ACTUAL_PRODUCTION) / 2 - SUM(TOTAL_REJECTS) - SUM(TOTAL_REWORKS) 
          ELSE SUM(ACTUAL_PRODUCTION) - SUM(TOTAL_REJECTS) - SUM(TOTAL_REWORKS) 
        END < 0
      )
      SELECT COUNT(*) AS RESULT FROM CTE
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)

  def test_total_downtime_greater_than_48_hrs(self):
    result = spark.sql("""
      WITH CTE AS (
      SELECT 
        DATE,
        MACHINE,
        PLANT
      FROM s_manufacturing.production_data_agg
      WHERE _DELETED IS FALSE
      GROUP BY
        DATE,
        MACHINE,
        PLANT
      HAVING SUM(TOTAL_DOWNTIME_MIN) > 2880
      )
      SELECT COUNT(*) AS RESULT FROM CTE
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)

  def test_total_downtime_negative(self):
    result = spark.sql("""
      WITH CTE AS (
        SELECT 
          DATE,
          MACHINE,
          PLANT
        FROM s_manufacturing.production_data_agg
        WHERE _DELETED IS FALSE
        GROUP BY
          DATE,
          MACHINE,
          PLANT
        HAVING SUM(TOTAL_DOWNTIME_MIN) < 0
      )
      SELECT COUNT(*) AS RESULT FROM CTE
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)

  def test_total_working_time_greater_than_48_hrs(self):
    result = spark.sql("""
      WITH CTE AS(
        SELECT 
          DATE,
          MACHINE,
          PLANT
        FROM s_manufacturing.production_data_agg
        WHERE _DELETED IS FALSE
        GROUP BY
          DATE,
          MACHINE,
          PLANT
        HAVING SUM(WORKING_TIME_MIN) > 2880
      )
      SELECT COUNT(*) AS RESULT FROM CTE
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)

  def test_total_working_negative(self):
    result = spark.sql("""
      WITH CTE AS (
        SELECT 
          DATE,
          MACHINE,
          PLANT
        FROM s_manufacturing.production_data_agg
        WHERE _DELETED IS FALSE
        GROUP BY
          DATE,
          MACHINE,
          PLANT
        HAVING SUM(WORKING_TIME_MIN) < 0
      )
      SELECT COUNT(*) AS RESULT FROM CTE
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)
    
  def test_working_time_divided_by_target_working_time_greater_than_1(self):
    result = spark.sql("""
      WITH CTE AS (
        SELECT 
          DATE,
          MACHINE,
          PLANT
        FROM s_manufacturing.production_data_agg
        WHERE _DELETED IS FALSE
        GROUP BY
          DATE,
          MACHINE,
          PLANT
        HAVING SUM(WORKING_TIME_MIN)/SUM(TARGET_WORKING_TIME_MIN) > 1
      )
      SELECT COUNT(*) AS RESULT FROM CTE
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)
    
  def test_bkk_axl_fpy_greater_than_1(self):
    result = spark.sql("""
      WITH CTE AS(
      SELECT 
        YEAR(DATE)*100+MONTH(DATE) AS YEARMONTH,
        PLANT
      FROM s_manufacturing.production_data_agg
      WHERE PLANT IN ('BKK','AXL') AND _DELETED IS FALSE
      GROUP BY
        YEAR(DATE)*100+MONTH(DATE),
        PLANT
      HAVING (SUM(ACTUAL_PRODUCTION)-SUM(TOTAL_REJECTS)-SUM(TOTAL_REWORKS))/SUM(ACTUAL_PRODUCTION) >1)
      SELECT COUNT(*) AS RESULT FROM CTE
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)
    
  def test_mlk_fpy_greater_than_1(self):
    result = spark.sql("""
      WITH CTE AS(
      SELECT 
        YEAR(DATE)*100+MONTH(DATE) AS YEARMONTH
      FROM s_manufacturing.production_data_agg
      WHERE PLANT ='MLK' AND _DELETED IS FALSE
      GROUP BY
        YEAR(DATE)*100+MONTH(DATE)
      HAVING SUM(NETT_DIPPING_PCS)/SUM(GROSS_DIPPING_PCS) * SUM(NET_PP_PCS)/ SUM(GROSS_PP_PCS) * SUM(BIN_PASS)/ SUM(BIN_TESTED) >1)
      SELECT COUNT(*) AS RESULT FROM CTE
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)
    
  def test_bkk_axl_rejects_greater_than_production(self):
    result = spark.sql("""
      WITH CTE AS(
      SELECT 
        YEAR(DATE)*100+MONTH(DATE) AS YEARMONTH,
        PLANT
      FROM s_manufacturing.production_data_agg
      WHERE PLANT IN ('BKK','AXL') AND _DELETED IS FALSE
      GROUP BY
        YEAR(DATE)*100+MONTH(DATE),
        PLANT
      HAVING SUM(TOTAL_REJECTS)/SUM(ACTUAL_PRODUCTION) >1)
      SELECT COUNT(*) AS RESULT FROM CTE
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0) 
    
  def test_mlk_rejects_greater_than_gross_dipping(self):
    result = spark.sql("""
      WITH CTE AS(
      SELECT 
        YEAR(DATE)*100+MONTH(DATE) AS YEARMONTH
      FROM s_manufacturing.production_data_agg
      WHERE PLANT ='MLK' AND _DELETED IS FALSE
      GROUP BY
        YEAR(DATE)*100+MONTH(DATE)
      HAVING SUM(TOTAL_REJECTS)/SUM(GROSS_DIPPING_PCS)/2 >1)
      SELECT COUNT(*) AS RESULT FROM CTE
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)
    
  def test_bkk_axl_reworks_greater_than_production(self):
    result = spark.sql("""
      WITH CTE AS(
      SELECT 
        YEAR(DATE)*100+MONTH(DATE) AS YEARMONTH,
        PLANT
      FROM s_manufacturing.production_data_agg
      WHERE PLANT IN ('BKK','AXL') AND _DELETED IS FALSE
      GROUP BY
        YEAR(DATE)*100+MONTH(DATE),
        PLANT
      HAVING SUM(TOTAL_REWORKS)/SUM(ACTUAL_PRODUCTION) >1)
      SELECT COUNT(*) AS RESULT FROM CTE
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0) 
    
  def test_mlk_reworks_greater_than_production(self):
    result = spark.sql("""
      WITH CTE AS(
      SELECT 
        YEAR(DATE)*100+MONTH(DATE) AS YEARMONTH
      FROM s_manufacturing.production_data_agg
      WHERE PLANT ='MLK' AND _DELETED IS FALSE
      GROUP BY
        YEAR(DATE)*100+MONTH(DATE)
      HAVING SUM(TOTAL_REWORKS)/SUM(GROSS_DIPPING_PCS)/2 >1)
      SELECT COUNT(*) AS RESULT FROM CTE
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0) 
  
  def test_bkk_mlk_availability_target_minus_downtime_greater_than_working_time(self):
    result = spark.sql("""
      WITH CTE AS(
      SELECT 
        YEAR(DATE)*100+MONTH(DATE) AS YEARMONTH,
        PLANT
      FROM s_manufacturing.production_data_agg
      WHERE PLANT IN('BKK','MLK') AND _DELETED IS FALSE
      GROUP BY
        YEAR(DATE)*100+MONTH(DATE),
        PLANT 
      HAVING (SUM(TARGET_WORKING_TIME_MIN)-SUM(TOTAL_DOWNTIME_MIN))/SUM(WORKING_TIME_MIN) >1)
      SELECT COUNT(*) AS RESULT FROM CTE
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)
    
  def test_bkk_quality_production_minus_rejects_reworks_divided_by_production_greater_than_1(self):
    result = spark.sql("""
      WITH CTE AS(
      SELECT 
        YEAR(DATE)*100+MONTH(DATE) AS YEARMONTH
      FROM s_manufacturing.production_data_agg
      WHERE PLANT='BKK' AND _DELETED IS FALSE
      GROUP BY
        YEAR(DATE)*100+MONTH(DATE)
      HAVING (SUM(ACTUAL_PRODUCTION)-SUM(TOTAL_REJECTS)-SUM(TOTAL_REWORKS))/ SUM(ACTUAL_PRODUCTION) >1)
      SELECT COUNT(*) AS RESULT FROM CTE
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)  
  
  def test_mlk_quality_production_minus_rejects_reworks_divided_by_production_greater_than_1(self):
    result = spark.sql("""
      WITH CTE AS(
      SELECT 
        YEAR(DATE)*100+MONTH(DATE) AS YEARMONTH
      FROM s_manufacturing.production_data_agg
      WHERE PLANT='MLK' AND _DELETED IS FALSE
      GROUP BY
        YEAR(DATE)*100+MONTH(DATE)
      HAVING (SUM(GROSS_DIPPING_PCS)/2 - SUM(TOTAL_REJECTS))/SUM(GROSS_DIPPING_PCS)/2 >1)
      SELECT COUNT(*) AS RESULT FROM CTE
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0) 
  
  def test_mlk_performance_production_greater_than_1_and_4(self):
    result = spark.sql("""
      WITH CTE AS(
      SELECT 
        YEAR(DATE)*100+MONTH(DATE) AS YEARMONTH
      FROM s_manufacturing.production_data_agg
      WHERE PLANT='MLK' AND _DELETED IS FALSE
      GROUP BY
        YEAR(DATE)*100+MONTH(DATE)
      HAVING (SUM(TARGET_WORKING_TIME_MIN)*SUM(GROSS_DIPPING_PCS)/2/SUM(TARGET_PRODUCTION))/(SUM(TARGET_WORKING_TIME_MIN)- SUM(TOTAL_DOWNTIME_MIN)) >1.4)
      SELECT COUNT(*) AS RESULT FROM CTE
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)  
    
  def test_bkk_axl_performance_production_greater_than_1_and_4(self):
    result = spark.sql("""
      WITH CTE AS(
      SELECT 
        YEAR(DATE)*100+MONTH(DATE) AS YEARMONTH,
        PLANT
      FROM s_manufacturing.production_data_agg
      WHERE PLANT IN('BKK','AXL') AND _DELETED IS FALSE
      GROUP BY
        YEAR(DATE)*100+MONTH(DATE),
        PLANT 
      HAVING SUM(ACTUAL_PRODUCTION)/SUM(TARGET_PRODUCTION) >1.4)
      SELECT COUNT(*) AS RESULT FROM CTE
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)
    
  def test_attainment_production_greater_than_1_and_5_daily(self):
    result = spark.sql("""
      WITH CTE AS(
      SELECT 
        DATE,
        PLANT
      FROM s_manufacturing.production_data_agg
      WHERE _DELETED IS FALSE
      GROUP BY
        DATE,
        PLANT 
      HAVING SUM(ACTUAL_PRODUCTION)/SUM(TARGET_PRODUCTION) >1.5)
      SELECT COUNT(*) AS RESULT FROM CTE
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)
    
    
  def test_bkk_axl_attainment_greater_than_1_and_5_daily(self):
    result = spark.sql("""
      WITH CTE AS(
      SELECT 
        DATE,
        PLANT
      FROM s_manufacturing.production_data_agg
      WHERE PLANT IN('BKK','AXL') AND _DELETED IS FALSE
      GROUP BY
        DATE,
        PLANT 
      HAVING SUM(ACTUAL_PRODUCTION)/SUM(TARGET_PRODUCTION) >1.5)
      SELECT COUNT(*) AS RESULT FROM CTE
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)   
    
  def test_axl_oee_greater_than_1_and_4(self):
    result = spark.sql("""
      WITH CTE AS(
      SELECT 
        YEAR(DATE)*100+MONTH(DATE) AS YEARMONTH
      FROM s_manufacturing.production_data_agg
      WHERE PLANT='AXL' AND _DELETED IS FALSE
      GROUP BY
        YEAR(DATE)*100+MONTH(DATE)
      HAVING SUM(ACTUAL_PRODUCTION)/SUM(TARGET_PRODUCTION) >1.4)
      SELECT COUNT(*) AS RESULT FROM CTE
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)
    
  def test_bkk_oee_greater_than_1and4(self):
    result = spark.sql("""
      WITH CTE AS(
      SELECT 
        YEAR(DATE)*100+MONTH(DATE) AS YEARMONTH
      FROM s_manufacturing.production_data_agg
      WHERE PLANT='BKK' AND _DELETED IS FALSE
      GROUP BY
        YEAR(DATE)*100+MONTH(DATE)
      HAVING (SUM(TARGET_WORKING_TIME_MIN)-SUM(TOTAL_DOWNTIME_MIN))/SUM(WORKING_TIME_MIN) * SUM(ACTUAL_PRODUCTION)/SUM(TARGET_PRODUCTION) * SUM(ACTUAL_PRODUCTION)-
            SUM(TOTAL_REJECTS)-SUM(TOTAL_REWORKS)/ SUM(ACTUAL_PRODUCTION) >1.4)
      SELECT COUNT(*) AS RESULT FROM CTE
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0) 
    
  def test_mlk_oee_greater_than_1and4(self):
    result = spark.sql("""
      WITH CTE AS(
      SELECT 
        YEAR(DATE)*100+MONTH(DATE) AS YEARMONTH
      FROM s_manufacturing.production_data_agg
      WHERE PLANT='MLK' AND _DELETED IS FALSE
      GROUP BY
        YEAR(DATE)*100+MONTH(DATE)
      HAVING (SUM(TARGET_WORKING_TIME_MIN)-SUM(TOTAL_DOWNTIME_MIN))/SUM(WORKING_TIME_MIN) * SUM(GROSS_DIPPING_PCS)/2 - SUM(TOTAL_REJECTS)/SUM(GROSS_DIPPING_PCS)/2 *
      (SUM(TARGET_WORKING_TIME_MIN)*SUM(GROSS_DIPPING_PCS)/2/SUM(TARGET_PRODUCTION))/(SUM(TARGET_WORKING_TIME_MIN)- SUM(TOTAL_DOWNTIME_MIN)) > 1.4)
      SELECT COUNT(*) AS RESULT FROM CTE
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)
    
  def test_downtime_greater_than_target(self):
    result = spark.sql("""
      WITH CTE AS(
      SELECT 
        DATE,
        MACHINE,
        PLANT
      FROM s_manufacturing.production_data_agg
      WHERE PLANT IN ('BKK','MLK') AND _DELETED IS FALSE
      GROUP BY
        DATE,
        MACHINE,
        PLANT
      HAVING SUM(TOTAL_DOWNTIME_MIN)/SUM(TARGET_WORKING_TIME_MIN) > 1)
      SELECT COUNT(*) AS RESULT FROM CTE
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)

# COMMAND ----------

suite = unittest.TestSuite()
suite.addTests(unittest.makeSuite(TestManufacturing))
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
