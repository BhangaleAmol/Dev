# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_silver_notebook

# COMMAND ----------

# DEFAULT PARAMETERS
database_name = database_name or "s_manufacturing"
table_name = table_name or "production_data_mlk"
target_folder = target_folder or "/datalake_silver/manufacturing/full_data"
source_name = "MLK"

# COMMAND ----------

# SETUP PARAMETERS
table_name = get_table_name(database_name, None, table_name)
table_name_agg = table_name[:table_name.rfind("_")] + '_agg'

# COMMAND ----------

# INPUT PARAMETERS
print_silver_params()

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'gpd.mlk_production_data')
  main_inc = load_incr_dataset('gpd.mlk_production_data', 'Modified', cutoff_value)
else:
  main_inc = load_full_dataset('gpd.mlk_production_data')

# COMMAND ----------

# SAMPLING
if sampling:
  main_inc = main_inc.limit(10)

# COMMAND ----------

# VIEWS
main_inc.createOrReplaceTempView('main_inc')

# COMMAND ----------

# MAGIC %sql USE GPD

# COMMAND ----------

MAIN_SP = spark.sql("""
  SELECT
    pd.Id AS ID,
    DATE_FORMAT(pd.Date, "yyyy-MM-dd") AS DATE,
    'MLK' AS PLANT,
    pl.GBU,
    pl.SBU,
    pd.MachineValue AS MACHINE,
    pd.Process AS PROCESS,
    pl.ProductGroup AS PRODUCT_GROUP,
    UPPER(REGEXP_REPLACE(pl.Product, "[^A-Za-z0-9_ ]", "")) AS PRODUCT,    
    CAST(pd.WorkingTimeMin AS INT) AS WORKING_TIME_MIN,
    CAST(pd.TargetWorkingTimeMin AS INT) AS TARGET_WORKING_TIME_MIN,
    CAST(IF(pd.TotalDowntimeMin < 0, 0, pd.TotalDowntimeMin) AS INT) AS TOTAL_DOWNTIME_MIN,
    CAST(pd.PlannedOutput AS INT) AS TARGET_PRODUCTION,
    DATE_FORMAT(pd.Created, "yyyy-MM-dd hh:mm:ss") AS CREATED,
    DATE_FORMAT(pd.Modified, "yyyy-MM-dd hh:mm:ss") AS MODIFIED
  FROM main_inc pd
  LEFT JOIN gpd.MLK_PRODUCTS_LIST pl ON pd.ProductId = pl.Id
  WHERE PRODUCT IS NOT NULL
""")

MAIN_SP.cache()
MAIN_SP.createOrReplaceTempView("MAIN_SP")
display(MAIN_SP)

# COMMAND ----------

# MAGIC %sql USE GPD_MLK

# COMMAND ----------

# MAGIC %md ANS_WORKORDER_DT

# COMMAND ----------

ANS_WORKORDER_DT_F = spark.sql("""
  SELECT
    IF(HOUR(d_atetime) < 12 AND shift = 3, TO_DATE(DATE_SUB(d_atetime, 1)), TO_DATE(d_atetime)) AS DATE, 
    * 
  FROM ANS_WORKORDER_DT
  WHERE status != 'Suspended'
""")
ANS_WORKORDER_DT_F.createOrReplaceTempView("ANS_WORKORDER_DT_F")
ANS_WORKORDER_DT_F.cache()
ANS_WORKORDER_DT_F.count()

# COMMAND ----------

# MAGIC %md ANS_WORKORDER_HD

# COMMAND ----------

ANS_WORKORDER_HD = spark.table("ANS_WORKORDER_HD")
ANS_WORKORDER_HD_F = (
  ANS_WORKORDER_HD
    .select(["runno", "productno", "machinecode"])
    .transform(trim_all_values)
    .withColumn("productno", f.regexp_replace(f.upper(f.col("productno")), "[^A-Za-z0-9_]", ""))
    .withColumnRenamed("productno", "PRODUCT_NO")
    .withColumnRenamed("machinecode", "MACHINE")
)
ANS_WORKORDER_HD_F.createOrReplaceTempView("ANS_WORKORDER_HD_F")
ANS_WORKORDER_HD_F.cache()
ANS_WORKORDER_HD_F.count()

# COMMAND ----------

# MAGIC %md ANS_PRODUCTMASTER

# COMMAND ----------

ANS_PRODUCTMASTER = spark.table("ANS_PRODUCTMASTER")
ANS_PRODUCTMASTER_F = (
  ANS_PRODUCTMASTER
    .transform(trim_all_values)
    .withColumn("productno", f.regexp_replace(f.col("productno"), "[^A-Za-z0-9_]", ""))
    .withColumn("productdesc2", f.regexp_replace(f.col("productdesc2"), "\\s+", " "))
    .withColumn("productdesc2", f.regexp_replace(f.col("productdesc2"), "[^A-Za-z0-9_ ]", "")) 
    .withColumn("productdesc2", f.trim(f.col("productdesc2"))) 
    .withColumn("productdesc2", f.upper(f.col("productdesc2"))) 
    .filter("productdesc2 <> ''")
    .filter("_DELETED IS FALSE")
    .withColumnRenamed('productno', 'PRODUCT_NO')
    .withColumnRenamed('productdesc2', 'PRODUCT')
    .select('runno', 'PRODUCT_NO', 'PRODUCT')
    .distinct()
)
ANS_PRODUCTMASTER_F.createOrReplaceTempView("ANS_PRODUCTMASTER_F")
ANS_PRODUCTMASTER_F.cache()
ANS_PRODUCTMASTER_F.count()

# COMMAND ----------

# MAGIC %md ANS_QA_DT

# COMMAND ----------

ANS_QA_DT = spark.table("ANS_QA_DT")
ANS_QA_DT_2 = (
  ANS_QA_DT.withColumn("trantype", f.regexp_replace(f.col("trantype"), "[^A-Za-z0-9_]", ""))
)

# COMMAND ----------

# MAGIC %md QRY_EXMACHINE

# COMMAND ----------

QRY_EXMACHINE = spark.sql("""
  SELECT
    dt.DATE,
    hd.MACHINE,    
    m.PRODUCT,
    CAST(SUM(dt.bwm_totalpcs) AS INT) AS NETT_DIPPING_PCS
  FROM ANS_WORKORDER_DT_F dt
  JOIN ANS_WORKORDER_HD_F hd ON dt.runno_hd = hd.runno
  JOIN ANS_PRODUCTMASTER_F m ON hd.PRODUCT_NO = m.PRODUCT_NO
  GROUP BY DATE, MACHINE, m.PRODUCT
""")
QRY_EXMACHINE.createOrReplaceTempView("QRY_EXMACHINE")
QRY_EXMACHINE.cache()
QRY_EXMACHINE.count()

# COMMAND ----------

# MAGIC %md ANS_QA_DT

# COMMAND ----------

ANS_QA_DT = spark.table("ANS_QA_DT")
ANS_QA_DT_2 = (
  ANS_QA_DT.withColumn("trantype", f.regexp_replace(f.col("trantype"), "[^A-Za-z0-9_]", ""))
)
ANS_QA_DT_2.createOrReplaceTempView("ANS_QA_DT_2")
ANS_QA_DT_2.count()

# COMMAND ----------

ANS_QA_DT_F = spark.sql("""
  SELECT 
    runno,
    runno_qa_hd,
    COALESCE(QA1, 0) AS QA1_REJECT_PCS,
    COALESCE(QA2, 0) AS QA2_REJECT_PCS, 
    COALESCE(QA3, 0) AS QA3_REJECT_PCS, 
    COALESCE(ReworkOut1, 0) AS RW1_REJECT_PCS, 
    COALESCE(ReworkOut2, 0) AS RW2_REJECT_PCS
  FROM (
    SELECT runno, runno_qa_hd, trantype, defect_qty
    FROM ANS_QA_DT_2
    WHERE 
      trantype IN (\"QA1\", \"QA2\", \"QA3\", \"ReworkOut1\", \"ReworkOut2\")
      AND defect_qty <> 0
  )
  PIVOT (
    SUM(defect_qty)
    FOR trantype IN (\"QA1\", \"QA2\", \"QA3\", \"ReworkOut1\", \"ReworkOut2\")
  )
""")

ANS_QA_DT_F.createOrReplaceTempView("ANS_QA_DT_F")
ANS_QA_DT_F.cache()
ANS_QA_DT_F.count()

# COMMAND ----------

# MAGIC %md ANS_QA_HD

# COMMAND ----------

ANS_QA_HD_F = spark.sql("""
  SELECT
    IF(HOUR(qhd.qa1completeddatetime) < 7 AND HOUR(qhd.qa1completeddatetime) >= 19,
      TO_DATE(DATE_SUB(qhd.qa1completeddatetime, 1)), 
      TO_DATE(qhd.qa1completeddatetime)) AS DATE,
    qhd.*
  FROM ANS_QA_HD qhd  
""")

ANS_QA_HD_F.createOrReplaceTempView("ANS_QA_HD_F")
ANS_QA_HD_F.cache()
ANS_QA_HD_F.count()

# COMMAND ----------

# MAGIC %md QRY_QA

# COMMAND ----------

QRY_QA = spark.sql("""
  SELECT 
    qhd.DATE, 
    whd.MACHINE,
    m.PRODUCT,
    SUM(qdt.QA1_REJECT_PCS) AS QA1_REJECT_PCS, 
    SUM(qdt.QA2_REJECT_PCS) AS QA2_REJECT_PCS, 
    SUM(qdt.QA3_REJECT_PCS) AS QA3_REJECT_PCS, 
    SUM(qdt.RW1_REJECT_PCS) AS RW1_REJECT_PCS, 
    SUM(qdt.RW2_REJECT_PCS) AS RW2_REJECT_PCS
  FROM ANS_QA_DT_F qdt 
  JOIN ANS_QA_HD_F qhd ON qhd.runno = qdt.runno_qa_hd
  JOIN ANS_WORKORDER_DT_F wdt ON qhd.bincardno1 = wdt.bincardno
  JOIN ANS_WORKORDER_HD_F whd ON wdt.runno_hd = whd.runno
  JOIN ANS_PRODUCTMASTER_F m ON whd.PRODUCT_NO = m.PRODUCT_NO
  WHERE qhd.qa1completedyn = 1
  GROUP BY qhd.DATE, whd.MACHINE, m.PRODUCT
""")
QRY_QA.createOrReplaceTempView("QRY_QA")
QRY_QA.cache()
QRY_QA.count()

# COMMAND ----------

# MAGIC %md ANS_POSTPROCESSING_PP01_PP05

# COMMAND ----------

ANS_POSTPROCESSING_PP01_PP05_F = spark.sql("""
  SELECT 
    IF(HOUR(pp.out01datetime) < 12 AND pp.shift = "Shift 3", 
      TO_DATE(DATE_SUB(pp.out01datetime, 1)), 
      TO_DATE(pp.out01datetime)) AS DATE, 
    pp.out01rejectqty AS PP_REJECT_PCS, 
    pp.out01reworkrejectqty AS REWORK_REJECT_PCS,
    pp.out01bincardno
  FROM ANS_POSTPROCESSING_PP01_PP05 pp
""")
ANS_POSTPROCESSING_PP01_PP05_F.createOrReplaceTempView("ANS_POSTPROCESSING_PP01_PP05_F")
ANS_POSTPROCESSING_PP01_PP05_F.cache()
ANS_POSTPROCESSING_PP01_PP05_F.count()

# COMMAND ----------

# MAGIC %md QRY_POSTPROCESSING

# COMMAND ----------

QRY_POSTPROCESSING = spark.sql("""
  SELECT
    pp.DATE, 
    whd.MACHINE,
    m.PRODUCT,
    SUM(pp.PP_REJECT_PCS) AS PP_REJECT_PCS, 
    SUM(pp.REWORK_REJECT_PCS) AS REWORK_REJECT_PCS
  FROM ANS_WORKORDER_DT_F wdt
  JOIN ANS_WORKORDER_HD_F whd ON wdt.runno_hd = whd.runno
  JOIN ANS_POSTPROCESSING_PP01_PP05_F pp ON wdt.bincardno = pp.out01bincardno
  JOIN ANS_PRODUCTMASTER_F m ON whd.PRODUCT_NO = m.PRODUCT_NO
  GROUP BY pp.DATE, whd.MACHINE, m.PRODUCT
""")

QRY_POSTPROCESSING.createOrReplaceTempView("QRY_POSTPROCESSING")
QRY_POSTPROCESSING.cache()
QRY_POSTPROCESSING.count()

# COMMAND ----------

# MAGIC %md ANS_FPXREJECT

# COMMAND ----------

ANS_FPXREJECT_F = spark.sql("""
  WITH ANS_FPXREJECT_2 AS (
    SELECT
    IF(HOUR(d_atetime) < 12 AND shift = "3", 
      TO_DATE(DATE_SUB(d_atetime, 1)), 
      TO_DATE(d_atetime)) AS DATE,
      machinecode AS MACHINE,
      runno,
      qcrejectqty,
      pfengrejectqty,
      pfrejectqty
    FROM ANS_FPXREJECT
  )
  SELECT 
    fpx.DATE, 
    fpx.MACHINE, 
    m.PRODUCT,
    SUM(fpx.qcrejectqty) AS OC_REJECT_PCS, 
    SUM(fpx.pfengrejectqty) AS ENGINEERING_REJECT_PCS, 
    SUM(fpx.pfrejectqty) AS PLATFORM_REJECT_PCS
  FROM ANS_FPXREJECT_2 fpx
  JOIN ANS_WORKORDER_DT_F wdt ON fpx.runno = wdt.runno
  JOIN ANS_WORKORDER_HD_F whd ON wdt.runno_hd = whd.runno
  JOIN ANS_PRODUCTMASTER_F m ON whd.PRODUCT_NO = m.PRODUCT_NO
  GROUP BY fpx.DATE, fpx.MACHINE, m.PRODUCT
""")

ANS_FPXREJECT_F.createOrReplaceTempView("ANS_FPXREJECT_F")
ANS_FPXREJECT_F.cache()
ANS_FPXREJECT_F.count()

# COMMAND ----------

# MAGIC %md ANS_PK_ET_HD

# COMMAND ----------

ANS_PK_ET_HD_F = spark.sql("""
  SELECT 
    IF(HOUR(pket.qacompleteddatetime) < 7 AND HOUR(pket.qacompleteddatetime) >= 19, 
      TO_DATE(DATE_SUB(pket.qacompleteddatetime, 1)), 
      TO_DATE(pket.qacompleteddatetime)) AS DATE,
      pket.runno,
      pket.qacompletedyn,
      pket.smcardno1
  FROM ANS_PK_ET_HD pket
  WHERE pket.qacompleteddatetime <> '1900-01-01T00:00:00.000+0000'
""")

ANS_PK_ET_HD_F.createOrReplaceTempView("ANS_PK_ET_HD_F")
ANS_PK_ET_HD_F.cache()
ANS_PK_ET_HD_F.count()
# display(ANS_PK_ET_HD_F)

# COMMAND ----------

# MAGIC %md QRY_PKET

# COMMAND ----------

QRY_PKET = spark.sql("""
  SELECT 
    pket.DATE,
    whd.MACHINE,
    m.PRODUCT,
    SUM(b.defect_qty) AS ET_REJECT_PCS
  FROM ANS_PK_ET_HD_F pket
  JOIN ans_pk_et_dt b ON pket.runno = b.runno_qa_hd
  JOIN ans_defect c ON b.runno_defect_hd = c.runno
  JOIN ans_supermarket_dt e ON pket.smcardno1 = e.smcardno1
  JOIN ans_supermarket_hd d ON d.runno = e.runno_hd 
  JOIN ANS_WORKORDER_DT_F f ON d.bincardno = f.bincardno
  JOIN ANS_WORKORDER_HD_F whd ON f.runno_hd = whd.runno
  JOIN ANS_PRODUCTMASTER_F m ON whd.PRODUCT_NO = m.PRODUCT_NO
  WHERE b.defect_qty <> 0 and pket.qacompletedyn = 1
  GROUP BY pket.DATE, whd.MACHINE, m.PRODUCT
""")

QRY_PKET.createOrReplaceTempView("QRY_PKET")
QRY_PKET.cache()
QRY_PKET.count()

# COMMAND ----------

# MAGIC %md ANS_PK_ISP_HD

# COMMAND ----------

ANS_PK_ISP_HD_F = spark.sql("""
  SELECT 
    IF(HOUR(pkisp.qacompleteddatetime) < 7 AND HOUR(pkisp.qacompleteddatetime) >= 19, 
      TO_DATE(DATE_SUB(pkisp.qacompleteddatetime, 1)), 
      TO_DATE(pkisp.qacompleteddatetime)) AS DATE,
    pkisp.runno,
    pkisp.smcardno1,
    pkisp.qacompletedyn
  FROM ANS_PK_ISP_HD pkisp
  WHERE pkisp.qacompleteddatetime <> '1900-01-01T00:00:00.000+0000'
""")

ANS_PK_ISP_HD_F.createOrReplaceTempView("ANS_PK_ISP_HD_F")
ANS_PK_ISP_HD_F.cache()
display(ANS_PK_ISP_HD_F)

# COMMAND ----------

# MAGIC %md QRY_PKISP

# COMMAND ----------

QRY_PKISP = spark.sql("""
  SELECT
    pkisp.DATE, 
    whd.MACHINE,
    m.PRODUCT,
    SUM(b.defect_qty) AS ISP_REJECT_PCS
  FROM ANS_PK_ISP_HD_F pkisp
  JOIN ans_pk_isp_dt b ON pkisp.runno = b.runno_qa_hd
  JOIN ans_defect c ON b.runno_defect_hd = c.runno
  JOIN ans_supermarket_dt e ON pkisp.smcardno1 = e.smcardno1
  JOIN ans_supermarket_hd d ON d.runno = e.runno_hd
  JOIN ANS_WORKORDER_DT_F f ON d.bincardno = f.bincardno
  JOIN ANS_WORKORDER_HD_F whd ON f.runno_hd = whd.runno
  JOIN ANS_PRODUCTMASTER_F m ON whd.PRODUCT_NO = m.PRODUCT_NO
  WHERE b.defect_qty <> 0 and pkisp.qacompletedyn = 1
  GROUP BY pkisp.DATE, whd.MACHINE, m.PRODUCT
""")

QRY_PKISP.createOrReplaceTempView("QRY_PKISP")
QRY_PKISP.cache()
QRY_PKISP.count()

# COMMAND ----------

# MAGIC %md BIN TESTED

# COMMAND ----------

BIN_TESTED = spark.sql("""
  SELECT
    qhd.DATE, 
    whd.MACHINE, 
    m.PRODUCT,
    COUNT(qhd.bincardno1) AS BIN_TESTED
  FROM ANS_QA_HD_F qhd
  JOIN ANS_WORKORDER_DT_F wdt ON qhd.bincardno1 = wdt.bincardno
  JOIN ANS_WORKORDER_HD_F whd ON whd.runno = wdt.runno_hd
  JOIN ANS_PRODUCTMASTER_F m ON whd.PRODUCT_NO = m.PRODUCT_NO
  WHERE qhd.qa1completedyn = 1
  GROUP BY qhd.DATE, whd.MACHINE, m.PRODUCT
""")
BIN_TESTED.createOrReplaceTempView("BIN_TESTED")
BIN_TESTED.cache()
display(BIN_TESTED)

# COMMAND ----------

# MAGIC %md BIN PASS

# COMMAND ----------

BIN_PASS = spark.sql("""
  SELECT
    qhd.DATE, 
    whd.MACHINE, 
    m.PRODUCT,
    COUNT(qhd.bincardno1) AS BIN_PASS
  FROM ANS_QA_HD_F qhd
  JOIN ANS_WORKORDER_DT_F wdt ON qhd.bincardno1 = wdt.bincardno
  JOIN ANS_WORKORDER_HD_F whd ON whd.runno = wdt.runno_hd
  JOIN ANS_PRODUCTMASTER_F m ON whd.PRODUCT_NO = m.PRODUCT_NO
  WHERE qhd.qa1completedyn = 1 AND qa1passyn = 1
  GROUP BY qhd.DATE, whd.MACHINE, m.PRODUCT
""")
BIN_PASS.createOrReplaceTempView("BIN_PASS")
BIN_PASS.cache()
display(BIN_PASS)

# COMMAND ----------

# MAGIC %md GROSS_PP

# COMMAND ----------

GROSS_PP = spark.sql("""
  SELECT
    pp.DATE, 
    whd.MACHINE, 
    m.PRODUCT,
    SUM(wdt.bwm_totalpcs) AS GROSS_PP_PCS
  FROM ANS_WORKORDER_DT_F wdt
  JOIN ANS_WORKORDER_HD_F whd ON wdt.runno_hd = whd.runno  
  JOIN ANS_POSTPROCESSING_PP01_PP05_F pp ON wdt.bincardno = pp.out01bincardno
  JOIN ANS_PRODUCTMASTER_F m ON whd.PRODUCT_NO = m.PRODUCT_NO
  GROUP BY pp.DATE, whd.MACHINE, m.PRODUCT
""")

GROSS_PP.createOrReplaceTempView("GROSS_PP")
GROSS_PP.cache()
display(GROSS_PP)

# COMMAND ----------

# MAGIC %md TWO SLOT MACHINES

# COMMAND ----------

QRY_EXMACHINE_TS = spark.sql("""
  WITH tsr AS (
    SELECT DATE, MACHINE, SUM(NETT_DIPPING_PCS) AS NETT_DIPPING_PCS
    FROM QRY_EXMACHINE
    GROUP BY DATE, MACHINE
    HAVING COUNT(*) > 1
  ) 
  SELECT 
    ex.*, 
    IF(COALESCE(tsr.NETT_DIPPING_PCS, 0) > 0, ex.NETT_DIPPING_PCS / tsr.NETT_DIPPING_PCS, 0) AS NETT_DIPPING_RATIO
  FROM QRY_EXMACHINE ex
  JOIN tsr ON ex.DATE = tsr.DATE AND ex.MACHINE = tsr.MACHINE
""")

QRY_EXMACHINE_TS.createOrReplaceTempView("QRY_EXMACHINE_TS")

# COMMAND ----------

QRY_EXMACHINE_TS_FLT = spark.sql("""
  WITH tsrf AS (
    SELECT exts.DATE, exts.MACHINE, exts.PRODUCT
    FROM QRY_EXMACHINE_TS exts
    JOIN MAIN_SP pd ON 
      exts.DATE = pd.DATE 
      AND exts.MACHINE = pd.MACHINE 
      AND exts.PRODUCT = pd.PRODUCT
  )
  SELECT exts.*
  FROM QRY_EXMACHINE_TS exts
  JOIN tsrf ON exts.DATE = tsrf.DATE AND exts.MACHINE = tsrf.MACHINE
""")

QRY_EXMACHINE_TS_FLT.createOrReplaceTempView("QRY_EXMACHINE_TS_FLT")

# COMMAND ----------

SP_TS_WITH_LAG = spark.sql("""
  SELECT
    pd.ID,
    COALESCE(
      LAG(pd.ID, -1) OVER (PARTITION BY ex.DATE, ex.MACHINE ORDER BY ex.DATE),
      LAG(pd.ID, 1) OVER (PARTITION BY ex.DATE, ex.MACHINE ORDER BY ex.DATE)
    ) AS LAG_ID,
    COALESCE(
      LAG(pd.GBU, -1) OVER (PARTITION BY ex.DATE, ex.MACHINE ORDER BY ex.DATE),
      LAG(pd.GBU, 1) OVER (PARTITION BY ex.DATE, ex.MACHINE ORDER BY ex.DATE)
    ) AS LAG_GBU,
    COALESCE(
      LAG(pd.SBU, -1) OVER (PARTITION BY ex.DATE, ex.MACHINE ORDER BY ex.DATE),
      LAG(pd.SBU, 1) OVER (PARTITION BY ex.DATE, ex.MACHINE ORDER BY ex.DATE)
    ) AS LAG_SBU,
    COALESCE(
      LAG(pd.PROCESS, -1) OVER (PARTITION BY ex.DATE, ex.MACHINE ORDER BY ex.DATE),
      LAG(pd.PROCESS, 1) OVER (PARTITION BY ex.DATE, ex.MACHINE ORDER BY ex.DATE)
    ) AS LAG_PROCESS,
    COALESCE(
      LAG(pd.WORKING_TIME_MIN, -1) OVER (PARTITION BY ex.DATE, ex.MACHINE ORDER BY ex.DATE),
      LAG(pd.WORKING_TIME_MIN, 1) OVER (PARTITION BY ex.DATE, ex.MACHINE ORDER BY ex.DATE)
    ) AS LAG_WORKING_TIME_MIN,
    COALESCE(
      LAG(pd.TARGET_WORKING_TIME_MIN, -1) OVER (PARTITION BY ex.DATE, ex.MACHINE ORDER BY ex.DATE),
      LAG(pd.TARGET_WORKING_TIME_MIN, 1) OVER (PARTITION BY ex.DATE, ex.MACHINE ORDER BY ex.DATE)
    ) AS LAG_TARGET_WORKING_TIME_MIN,
    COALESCE(
      LAG(pd.TOTAL_DOWNTIME_MIN, -1) OVER (PARTITION BY ex.DATE, ex.MACHINE ORDER BY ex.DATE),
      LAG(pd.TOTAL_DOWNTIME_MIN, 1) OVER (PARTITION BY ex.DATE, ex.MACHINE ORDER BY ex.DATE)
    ) AS LAG_TOTAL_DOWNTIME_MIN,
    COALESCE(
      LAG(pd.TARGET_PRODUCTION, -1) OVER (PARTITION BY ex.DATE, ex.MACHINE ORDER BY ex.DATE),
      LAG(pd.TARGET_PRODUCTION, 1) OVER (PARTITION BY ex.DATE, ex.MACHINE ORDER BY ex.DATE)
    ) AS LAG_TARGET_PRODUCTION,
    COALESCE(
      LAG(pd.CREATED, -1) OVER (PARTITION BY ex.DATE, ex.MACHINE ORDER BY ex.DATE),
      LAG(pd.CREATED, 1) OVER (PARTITION BY ex.DATE, ex.MACHINE ORDER BY ex.DATE)
    ) AS LAG_CREATED,
    COALESCE(
      LAG(pd.MODIFIED, -1) OVER (PARTITION BY ex.DATE, ex.MACHINE ORDER BY ex.DATE),
      LAG(pd.MODIFIED, 1) OVER (PARTITION BY ex.DATE, ex.MACHINE ORDER BY ex.DATE)
    ) AS LAG_MODIFIED,
    ex.*,
    pd.GBU,
    pd.SBU,
    pd.PROCESS,
    pd.PRODUCT_GROUP,
    pd.WORKING_TIME_MIN,
    pd.TARGET_WORKING_TIME_MIN,
    pd.TOTAL_DOWNTIME_MIN,
    pd.TARGET_PRODUCTION,
    pd.CREATED,
    pd.MODIFIED
  FROM QRY_EXMACHINE_TS_FLT ex
  LEFT JOIN MAIN_SP pd ON ex.DATE = pd.DATE AND ex.MACHINE = pd.MACHINE AND ex.PRODUCT = pd.PRODUCT
""")

SP_TS_WITH_LAG.createOrReplaceTempView("SP_TS_WITH_LAG")

# COMMAND ----------

SP_TS_F = spark.sql("""
  SELECT 
    COALESCE(spl.ID, spl.LAG_ID || '-2') AS ID,
    spl.DATE,
    'MLK' AS PLANT,
    COALESCE(spl.GBU, spl.LAG_GBU) AS GBU,
    COALESCE(spl.SBU, spl.LAG_SBU) AS SBU,
    spl.MACHINE,
    COALESCE(spl.PROCESS, spl.LAG_PROCESS) AS PROCESS,
    pl.ProductGroup AS PRODUCT_GROUP,
    spl.PRODUCT,
    ROUND(COALESCE(spl.WORKING_TIME_MIN, spl.LAG_WORKING_TIME_MIN) * spl.NETT_DIPPING_RATIO) AS WORKING_TIME_MIN,
    ROUND(COALESCE(spl.TARGET_WORKING_TIME_MIN, spl.LAG_TARGET_WORKING_TIME_MIN) * spl.NETT_DIPPING_RATIO) AS TARGET_WORKING_TIME_MIN,
    ROUND(COALESCE(spl.TOTAL_DOWNTIME_MIN, spl.LAG_TOTAL_DOWNTIME_MIN) * spl.NETT_DIPPING_RATIO) AS TOTAL_DOWNTIME_MIN,
    ROUND(COALESCE(spl.TARGET_PRODUCTION, spl.LAG_TARGET_PRODUCTION) * spl.NETT_DIPPING_RATIO) AS TARGET_PRODUCTION,
    COALESCE(spl.CREATED, spl.LAG_CREATED) AS CREATED,
    COALESCE(spl.MODIFIED, spl.LAG_MODIFIED) AS MODIFIED
  FROM SP_TS_WITH_LAG spl
  LEFT JOIN MAIN_SP pd ON spl.DATE = pd.DATE AND spl.MACHINE = pd.MACHINE AND spl.PRODUCT = pd.PRODUCT
  LEFT JOIN gpd.MLK_PRODUCTS_LIST pl ON spl.PRODUCT = pl.Product
  WHERE pd.DATE IS NULL AND pl._DELETED IS FALSE
""")

SP_TS_F.createOrReplaceTempView("SP_TS_F")

# COMMAND ----------

# MAGIC %md MAIN

# COMMAND ----------

MAIN_SP_F = spark.sql("""
  SELECT * FROM MAIN_SP
  UNION
  SELECT * FROM SP_TS_F
""")
MAIN_SP_F.createOrReplaceTempView("MAIN_SP_F")

# COMMAND ----------

MAIN = spark.sql("""
  SELECT 
    pd.*,
    0 AS NETT_DIPPING_PCS,    
    0 AS OC_REJECT_PCS,
    0 AS ENGINEERING_REJECT_PCS,
    0 AS PLATFORM_REJECT_PCS,
    0 AS QA1_REJECT_PCS,
    0 AS QA2_REJECT_PCS,
    0 AS QA3_REJECT_PCS,
    0 AS RW1_REJECT_PCS,
    0 AS RW2_REJECT_PCS,
    0 AS PP_REJECT_PCS,
    0 AS REWORK_REJECT_PCS,
    0 AS ET_REJECT_PCS,
    0 AS ISP_REJECT_PCS,
    0 AS BIN_TESTED,
    0 AS BIN_PASS,
    0 AS GROSS_PP_PCS 
  FROM MAIN_SP_F pd
  LEFT JOIN QRY_EXMACHINE ex ON ex.DATE = pd.DATE AND ex.MACHINE = pd.MACHINE 
  WHERE ex.PRODUCT IS NULL

  UNION

  SELECT 
    pd.*,
    COALESCE(ex.NETT_DIPPING_PCS, 0) AS NETT_DIPPING_PCS,    
    COALESCE(fpx.OC_REJECT_PCS, 0) AS OC_REJECT_PCS,
    COALESCE(fpx.ENGINEERING_REJECT_PCS, 0) AS ENGINEERING_REJECT_PCS,
    COALESCE(fpx.PLATFORM_REJECT_PCS, 0) AS PLATFORM_REJECT_PCS,
    COALESCE(qa.QA1_REJECT_PCS, 0) AS QA1_REJECT_PCS,
    COALESCE(qa.QA2_REJECT_PCS, 0) AS QA2_REJECT_PCS,
    COALESCE(qa.QA3_REJECT_PCS, 0) AS QA3_REJECT_PCS,
    COALESCE(qa.RW1_REJECT_PCS, 0) AS RW1_REJECT_PCS,
    COALESCE(qa.RW2_REJECT_PCS, 0) AS RW2_REJECT_PCS,
    COALESCE(pp.PP_REJECT_PCS, 0) AS PP_REJECT_PCS,
    COALESCE(pp.REWORK_REJECT_PCS, 0) AS REWORK_REJECT_PCS,
    COALESCE(et.ET_REJECT_PCS, 0) AS ET_REJECT_PCS,
    COALESCE(isp.ISP_REJECT_PCS, 0) AS ISP_REJECT_PCS,
    COALESCE(bt.BIN_TESTED, 0) AS BIN_TESTED,
    COALESCE(bp.BIN_PASS, 0) AS BIN_PASS,
    COALESCE(gpp.GROSS_PP_PCS, 0) AS GROSS_PP_PCS    
  FROM MAIN_SP_F pd
  JOIN QRY_EXMACHINE ex ON ex.DATE = pd.DATE AND ex.MACHINE = pd.MACHINE AND ex.PRODUCT = pd.PRODUCT  
  LEFT JOIN ANS_FPXREJECT_F fpx ON ex.DATE = fpx.DATE AND ex.MACHINE = fpx.MACHINE AND ex.PRODUCT = fpx.PRODUCT
  LEFT JOIN QRY_QA qa ON ex.DATE = qa.DATE AND ex.MACHINE = qa.MACHINE AND ex.PRODUCT = qa.PRODUCT
  LEFT JOIN QRY_POSTPROCESSING pp ON ex.DATE = pp.DATE AND ex.MACHINE = pp.MACHINE AND ex.PRODUCT = pp.PRODUCT 
  LEFT JOIN QRY_PKET et ON qa.DATE = et.DATE AND qa.MACHINE = et.MACHINE AND qa.PRODUCT = et.PRODUCT
  LEFT JOIN QRY_PKISP isp ON qa.DATE = isp.DATE AND qa.MACHINE = isp.MACHINE AND qa.PRODUCT = isp.PRODUCT
  LEFT JOIN BIN_TESTED bt ON ex.DATE = bt.DATE AND ex.MACHINE = bt.MACHINE AND ex.PRODUCT = bt.PRODUCT
  LEFT JOIN BIN_PASS bp ON ex.DATE = bp.DATE AND ex.MACHINE = bp.MACHINE AND ex.PRODUCT = bp.PRODUCT
  LEFT JOIN GROSS_PP gpp ON ex.DATE = gpp.DATE AND ex.MACHINE = gpp.MACHINE AND ex.PRODUCT = gpp.PRODUCT
""")

MAIN.createOrReplaceTempView("MAIN")
MAIN.persist(StorageLevel.DISK_ONLY)
display(MAIN)

# COMMAND ----------

# MAGIC %md NOTIFICATIONS

# COMMAND ----------

# NOTIFICATION - RECORDS NOT UPDATED

if(ENV_NAME == 'prod'):
  
  LAST_RECORD = spark.sql("""
    SELECT
      DATEDIFF(CURRENT_DATE(), MAX(DATE)) AS LAST_RECORD_DAYS,
      MAX(DATE) AS LAST_RECORD_DATE
    FROM MAIN_SP_F
  """).collect()[0]

  if LAST_RECORD['LAST_RECORD_DAYS'] > 7:
    data = {
      "site": "MLK",
      "list_name": 'MELAKA_PRODUCTION_DATA',
      "max_date": LAST_RECORD['LAST_RECORD_DATE']    
    }
    mail_gpd_records_not_updated(**data)

# COMMAND ----------

# NOTIFICATION - RECORDS NOT MATCHING

if(ENV_NAME == 'prod'):

  NO_MATCH_SP = spark.sql("""
    SELECT
      pd.ID, pd.DATE, pd.PLANT, pd.MACHINE, pd.PRODUCT
    FROM MAIN_SP_F pd
    LEFT JOIN MAIN m ON pd.ID = m.ID
    WHERE m.ID IS NULL
    ORDER BY ID
  """)

  NO_MATCH_SP.cache()
  NO_MATCH_SP.createOrReplaceTempView("NO_MATCH_SP")
  NO_MATCH_SP.count()

  NO_MATCH_DB = spark.sql("""
    SELECT ex.DATE, ex.MACHINE, ex.PRODUCT 
    FROM QRY_EXMACHINE ex
    JOIN NO_MATCH_SP nm ON ex.DATE = nm.DATE AND ex.MACHINE = nm.MACHINE
    ORDER BY ID
  """)

  NO_MATCH_DB.cache()
  NO_MATCH_DB.createOrReplaceTempView("NO_MATCH_DB")
  NO_MATCH_DB.count()

  if NO_MATCH_SP.count() > 0:
    data = {
      "site": "MLK",
      "list_name": 'MELAKA_PRODUCTION_DATA',
      "sp_html": NO_MATCH_SP.toPandas().to_html(),
      "db_html": NO_MATCH_DB.toPandas().to_html()    
    }

    mail_gpd_records_not_matching(**data)

# COMMAND ----------

MAIN_REDUCED = spark.sql("""
   SELECT
      ID,
      DATE,
      PLANT,
      GBU,
      SBU,
      MACHINE,
      PROCESS,
      PRODUCT_GROUP,
      PRODUCT,
      NETT_DIPPING_PCS,
      TARGET_PRODUCTION,
      WORKING_TIME_MIN,
      TARGET_WORKING_TIME_MIN,
      TOTAL_DOWNTIME_MIN,      
      OC_REJECT_PCS + ENGINEERING_REJECT_PCS + PLATFORM_REJECT_PCS + PP_REJECT_PCS + REWORK_REJECT_PCS + QA1_REJECT_PCS + QA2_REJECT_PCS + QA3_REJECT_PCS + 
      RW1_REJECT_PCS + RW2_REJECT_PCS + ET_REJECT_PCS + ISP_REJECT_PCS AS TOTAL_REJECTS,
      BIN_TESTED - BIN_PASS AS TOTAL_REWORKS,
      CREATED,
      MODIFIED,
      NETT_DIPPING_PCS + OC_REJECT_PCS + ENGINEERING_REJECT_PCS + PLATFORM_REJECT_PCS AS ACTUAL_PRODUCTION,
      BIN_PASS,
      BIN_TESTED,
      GROSS_PP_PCS - PP_REJECT_PCS + REWORK_REJECT_PCS AS NET_PP_PCS,
      GROSS_PP_PCS,
      NETT_DIPPING_PCS + OC_REJECT_PCS + ENGINEERING_REJECT_PCS + PLATFORM_REJECT_PCS AS GROSS_DIPPING_PCS
    FROM MAIN
""")

MAIN_REDUCED.createOrReplaceTempView("MAIN_REDUCED")
MAIN_REDUCED.cache()
MAIN_REDUCED.display()

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_manufacturing.production_data

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  MAIN_REDUCED
  .transform(attach_deleted_flag(False))
  .transform(attach_modified_date())
  .transform(attach_source_column(source = source_name, column = "_SOURCE"))
  .transform(attach_surrogate_key(columns = 'ID,_SOURCE'))
  .transform(attach_partition_column("CREATED", 'yyyy-MM-dd HH:mm:ss', 'yyyy-MM'))
  .transform(apply_schema(schema))
)

main_f.cache()
main_f.createOrReplaceTempView("main_f")
display(main_f)

# COMMAND ----------

# VALIDATE DATASET
valid_count_rows(main_f , "_ID")

# COMMAND ----------

# PERSIST DATA
merge_to_delta(main_f, table_name, target_folder, overwrite)

options = {'auto_merge': True, 'overwrite': overwrite}
merge_to_delta_agg(main_f, table_name_agg, source_name, target_folder, options)

# COMMAND ----------

# HANDLE DELETE
full_keys_f = (
  spark.table('gpd.mlk_production_data')
  .filter("_DELETED IS FALSE")
  .select("ID")
  .transform(attach_source_column(source = source_name))
  .transform(attach_surrogate_key(columns = 'ID,_SOURCE'))
  .select('_ID')
)

apply_soft_delete(full_keys_f, table_name, key_columns = '_ID')
apply_soft_delete(full_keys_f, table_name_agg, key_columns = '_ID', source_name = source_name)

# COMMAND ----------

main_f.unpersist()
MAIN.unpersist()
MAIN_REDUCED.unpersist()
BIN_PASS.unpersist()
BIN_TESTED.unpersist()
QRY_PKISP.unpersist()
ANS_PK_ISP_HD_F.unpersist()
QRY_PKET.unpersist()
ANS_PK_ET_HD_F.unpersist()
ANS_FPXREJECT_F.unpersist()
QRY_POSTPROCESSING.unpersist()
ANS_POSTPROCESSING_PP01_PP05_F.unpersist()
QRY_QA.unpersist()
ANS_QA_HD_F.unpersist()
ANS_QA_DT_F.unpersist()
QRY_EXMACHINE.unpersist()
ANS_PRODUCTMASTER_F.unpersist()
ANS_WORKORDER_HD_F.unpersist()
ANS_WORKORDER_DT_F.unpersist()
GROSS_PP.unpersist()

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not test_run:
  cutoff_value = get_incr_col_max_value(main_inc, "Modified")
  update_cutoff_value(cutoff_value, table_name, 'gpd.mlk_production_data')
  update_run_datetime(run_datetime, table_name, 'gpd.mlk_production_data')
