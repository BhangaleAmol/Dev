# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap 

# COMMAND ----------

# INPUT PARAMETERS
temp_folder = get_input_param('temp_folder', default_value = '/datalake_silver/callsystems/temp_data')
table_name = get_input_param('table_name', default_value = 'company_bridge')

# COMMAND ----------

file_name = get_file_name(table_name, 'dlt')
target_file = get_file_path(temp_folder, file_name)
print(file_name)
print(target_file)

# COMMAND ----------

file_path = get_file_path(temp_folder, 'clean_csrs.dlt')
csrs_df = spark.read.format('delta').load(file_path)
csrs_df.createOrReplaceTempView("csrs_df")

file_path = get_file_path(temp_folder, 'clean_sf_account.dlt')
sf_account_df = spark.read.format('delta').load(file_path)
sf_account_df.createOrReplaceTempView("sf_account_df")

file_path = get_file_path(temp_folder, 'clean_sf_contact.dlt')
sf_contact_df = spark.read.format('delta').load(file_path)
sf_contact_df.createOrReplaceTempView("sf_contact_df")

# COMMAND ----------

SF = spark.sql("""
  SELECT DISTINCT 
    A.ACCOUNT_ID AS COMPANY_ID, 
    A.ACCOUNT_NAME AS COMPANY_NAME,
    A.BILLING_COUNTRY AS COUNTRY,
    C.PHONE_NUMBER, 
    C.MOBILE_NUMBER,
    A.LAST_MODIFIED_DATE
  FROM sf_account_df A
  LEFT JOIN sf_contact_df C ON C.ACCOUNT_ID = A.ACCOUNT_ID
""")

SF.createOrReplaceTempView("SF")
display(SF)

# COMMAND ----------

SF3 = spark.sql("""
  SELECT DISTINCT COMPANY_ID, COMPANY_NAME, COUNTRY, PHONE, LAST_MODIFIED_DATE
  FROM (
    SELECT COMPANY_ID, COMPANY_NAME, COUNTRY, PHONE_NUMBER AS PHONE, LAST_MODIFIED_DATE
    FROM SF 
    WHERE PHONE_NUMBER IS NOT NULL
    UNION
    SELECT COMPANY_ID, COMPANY_NAME, COUNTRY, MOBILE_NUMBER AS PHONE, LAST_MODIFIED_DATE
    FROM SF 
    WHERE MOBILE_NUMBER IS NOT NULL
  )
""")

SF3.persist(StorageLevel.DISK_ONLY)
SF3.createOrReplaceTempView("SF3")
display(SF3)

# COMMAND ----------

SF4 = spark.sql("""
  SELECT COMPANY_ID, COMPANY_NAME, COUNTRY, PHONE
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY PHONE ORDER BY LAST_MODIFIED_DATE DESC) AS _NUM 
    FROM SF3
  )
  WHERE _NUM = 1
""")

SF4.persist(StorageLevel.DISK_ONLY)
SF4.createOrReplaceTempView("SF4")
display(SF4)

# COMMAND ----------

CSRS2 = (
  csrs_df
  .select('SID', 'ANI', 'DNIS', 'CALL_DIRECTION')
  .filter('DELETED_RECORD == 0 or DELETED_RECORD IS NULL')
)

CSRS2.createOrReplaceTempView("CSRS2")
print(CSRS2.count())

# COMMAND ----------

DF = spark.sql("""
  WITH CTI AS (
    SELECT SID, ANI, DNIS, CALL_DIRECTION
    FROM CSRS2
    WHERE 
        (CSRS2.ANI IS NOT NULL AND CSRS2.CALL_DIRECTION = 'inbound')
        OR (CSRS2.DNIS IS NOT NULL AND CSRS2.CALL_DIRECTION = 'outdial')
  )
  SELECT CTI.*, SF.COMPANY_ID, SF.COMPANY_NAME
  FROM CTI
  LEFT JOIN SF4 SF ON SF.PHONE = CTI.ANI
  WHERE CTI.CALL_DIRECTION = 'inbound'
  UNION
  SELECT CTI.*, SF.COMPANY_ID, SF.COMPANY_NAME
  FROM CTI
  LEFT JOIN SF4 SF ON SF.PHONE = CTI.DNIS
  WHERE CTI.CALL_DIRECTION = 'outdial'
""")

DF.persist(StorageLevel.DISK_ONLY)
DF.createOrReplaceTempView("DF")
display(DF)

# COMMAND ----------

DF_NULL = DF.filter(DF.COMPANY_NAME.isNull())
DF_NULL.persist(StorageLevel.DISK_ONLY)
DF_NULL.createOrReplaceTempView("DF_NULL")

DF_VAL = DF.filter(DF.COMPANY_NAME.isNotNull())
DF_VAL.persist(StorageLevel.DISK_ONLY)
DF_VAL.createOrReplaceTempView("DF_VAL")
print(DF_VAL.count())

# COMMAND ----------

RESULT_DF = spark.sql("""
   SELECT 
     DF.SID, DF.ANI, DF.DNIS, DF.CALL_DIRECTION, 
     COALESCE(DF.COMPANY_ID, SF.COMPANY_ID) AS COMPANY_ID, 
     COALESCE(DF.COMPANY_NAME, SF.COMPANY_NAME) AS COMPANY_NAME
   FROM DF_NULL DF 
   LEFT JOIN SF4 SF ON DF.ANI = SUBSTRING(SF.PHONE, 2, LENGTH(SF.PHONE) - 1)
   WHERE DF.CALL_DIRECTION = 'inbound'
   UNION
   SELECT 
     DF.SID, DF.ANI, DF.DNIS, DF.CALL_DIRECTION,
     COALESCE(DF.COMPANY_ID, SF.COMPANY_ID) AS COMPANY_ID, 
     COALESCE(DF.COMPANY_NAME, SF.COMPANY_NAME) AS COMPANY_NAME
   FROM DF_NULL DF 
   LEFT JOIN SF4 SF ON DF.DNIS = SUBSTRING(SF.PHONE, 2, LENGTH(SF.PHONE) - 1)
   WHERE DF.CALL_DIRECTION = 'outdial'
""")

DF_NULL2 = RESULT_DF.filter(RESULT_DF.COMPANY_NAME.isNull())
DF_NULL2.persist(StorageLevel.DISK_ONLY) 
DF_NULL2.createOrReplaceTempView("DF_NULL2")

DF_VAL2 = DF_VAL.union(RESULT_DF.filter(RESULT_DF.COMPANY_NAME.isNotNull())) 
DF_VAL2.persist(StorageLevel.DISK_ONLY) 
print(DF_VAL2.count())

# COMMAND ----------

RESULT_DF = spark.sql("""
   SELECT 
     DF.SID, DF.ANI, DF.DNIS, DF.CALL_DIRECTION, 
     COALESCE(DF.COMPANY_ID, SF.COMPANY_ID) AS COMPANY_ID, 
     COALESCE(DF.COMPANY_NAME, SF.COMPANY_NAME) AS COMPANY_NAME
   FROM DF_NULL2 DF 
   LEFT JOIN SF4 SF ON DF.ANI = SUBSTRING(SF.PHONE, 3, LENGTH(SF.PHONE) - 2)
   WHERE DF.CALL_DIRECTION = 'inbound'
   UNION
   SELECT 
     DF.SID, DF.ANI, DF.DNIS, DF.CALL_DIRECTION, 
     COALESCE(DF.COMPANY_ID, SF.COMPANY_ID) AS COMPANY_ID, 
     COALESCE(DF.COMPANY_NAME, SF.COMPANY_NAME) AS COMPANY_NAME
   FROM DF_NULL2 DF 
   LEFT JOIN SF4 SF ON DF.DNIS = SUBSTRING(SF.PHONE, 3, LENGTH(SF.PHONE) - 2)
   WHERE DF.CALL_DIRECTION = 'outdial'
""")

DF_NULL3 = RESULT_DF.filter(RESULT_DF.COMPANY_NAME.isNull())
DF_NULL3.persist(StorageLevel.DISK_ONLY) 
DF_NULL3.createOrReplaceTempView("DF_NULL3")

DF_VAL3 = DF_VAL2.union(RESULT_DF.filter(RESULT_DF.COMPANY_NAME.isNotNull())) 
DF_VAL3.persist(StorageLevel.DISK_ONLY) 
print(DF_VAL3.count())

# COMMAND ----------

RESULT_DF = spark.sql("""
   SELECT
     DF.SID, DF.ANI, DF.DNIS, DF.CALL_DIRECTION, 
     COALESCE(DF.COMPANY_ID, SF.COMPANY_ID) AS COMPANY_ID, 
     COALESCE(DF.COMPANY_NAME, SF.COMPANY_NAME) AS COMPANY_NAME
   FROM DF_NULL3 DF 
   LEFT JOIN SF4 SF ON SUBSTRING(DF.ANI, 3, LENGTH(DF.ANI) - 2) = SF.PHONE
   WHERE DF.CALL_DIRECTION = 'inbound'
   UNION
   SELECT
     DF.SID, DF.ANI, DF.DNIS, DF.CALL_DIRECTION, 
     COALESCE(DF.COMPANY_ID, SF.COMPANY_ID) AS COMPANY_ID, 
     COALESCE(DF.COMPANY_NAME, SF.COMPANY_NAME) AS COMPANY_NAME
   FROM DF_NULL3 DF 
   LEFT JOIN SF4 SF ON SUBSTRING(DF.DNIS, 3, LENGTH(DF.DNIS) - 2) = SF.PHONE
   WHERE DF.CALL_DIRECTION = 'outdial'
""")

DF_NULL4 = RESULT_DF.filter(RESULT_DF.COMPANY_NAME.isNull())
DF_NULL4.persist(StorageLevel.DISK_ONLY) 
DF_NULL4.createOrReplaceTempView("DF_NULL4")

DF_VAL4 = DF_VAL3.union(RESULT_DF.filter(RESULT_DF.COMPANY_NAME.isNotNull())) 
DF_VAL4.persist(StorageLevel.DISK_ONLY) 
DF_VAL4.createOrReplaceTempView("DF_VAL4")
print(DF_VAL4.count())

# COMMAND ----------

DF_FIN = DF_VAL4.union(DF_NULL4)
DF_FIN.persist(StorageLevel.DISK_ONLY)
DF_FIN.createOrReplaceTempView("DF_FIN")
DF_FIN.take(1)

# COMMAND ----------

BRIDGE_DF1 = DF_VAL4.select('SID', 'COMPANY_ID').distinct()
BRIDGE_DF1.count()

# COMMAND ----------

RESULT_DF = (
  DF_NULL4
  .select('SID', 'ANI', 'DNIS', 'CALL_DIRECTION')
  .filter('CALL_DIRECTION = "inbound"')
  .join(SF4, SF4.PHONE.contains(DF_NULL4.ANI), how='left')
)

RESULT_DF2 = (
  RESULT_DF
  .select('SID', 'COMPANY_ID')
  .where(RESULT_DF.COMPANY_ID.isNotNull())
  .where(f.length(RESULT_DF.ANI) > 7)
  .distinct()
)

BRIDGE_DF2 = (
  RESULT_DF2
  .groupBy('SID', 'COMPANY_ID')
  .count()
  .where('count == 1')
  .drop('count')
)

BRIDGE_DF2.persist(StorageLevel.DISK_ONLY)
BRIDGE_DF2.count()

# COMMAND ----------

RESULT_DF = (
  DF_NULL4
  .select('SID', 'ANI', 'DNIS', 'CALL_DIRECTION')
  .filter('CALL_DIRECTION = "outdial"')
  .join(SF4, SF4.PHONE.contains(DF_NULL4.DNIS), how='left')
)

RESULT_DF2 = (
  RESULT_DF
  .select('SID', 'COMPANY_ID')
  .where(RESULT_DF.COMPANY_ID.isNotNull())
  .where(f.length(RESULT_DF.DNIS) > 7)
  .distinct()
)

BRIDGE_DF3 = (
  RESULT_DF2
  .groupBy('SID', 'COMPANY_ID')
  .count()
  .where('count == 1')
  .drop('count')
)

BRIDGE_DF3.persist(StorageLevel.DISK_ONLY)
BRIDGE_DF3.count()

# COMMAND ----------

RESULT_DF = (
  DF_NULL4
  .select('SID', 'ANI', 'DNIS', 'CALL_DIRECTION')
  .filter('CALL_DIRECTION = "inbound"')
  .join(SF4, DF_NULL4.ANI.contains(SF4.PHONE), how='left')
)

RESULT_DF2 = (
  RESULT_DF
  .select('SID', 'COMPANY_ID')
  .where(RESULT_DF.COMPANY_ID.isNotNull())
  .where(f.length(RESULT_DF.ANI) > 7)
  .distinct()
)

BRIDGE_DF4 = (
  RESULT_DF2
  .groupBy('SID', 'COMPANY_ID')
  .count()
  .where('count == 1')
  .drop('count')
)

BRIDGE_DF4.persist(StorageLevel.DISK_ONLY)
BRIDGE_DF4.count()

# COMMAND ----------

RESULT_DF = (
  DF_NULL4
  .select('SID', 'ANI', 'DNIS', 'CALL_DIRECTION')
  .filter('CALL_DIRECTION = "outdial"')
  .join(SF4, DF_NULL4.DNIS.contains(SF4.PHONE), how='left')
)

RESULT_DF2 = (
  RESULT_DF
  .select('SID', 'COMPANY_ID')
  .where(RESULT_DF.COMPANY_ID.isNotNull())
  .where(f.length(RESULT_DF.DNIS) > 7)
  .distinct()
)

BRIDGE_DF5 = (
  RESULT_DF2
  .groupBy('SID', 'COMPANY_ID')
  .count()
  .where('count == 1')
  .drop('count')
)

BRIDGE_DF5.persist(StorageLevel.DISK_ONLY)
BRIDGE_DF5.count()

# COMMAND ----------

BRIDGE_DF = BRIDGE_DF1.union(BRIDGE_DF2).union(BRIDGE_DF3).union(BRIDGE_DF4).union(BRIDGE_DF5)
BRIDGE_DF.createOrReplaceTempView('BRIDGE_DF')
#BRIDGE_DF.persist(StorageLevel.DISK_ONLY)
#BRIDGE_DF.unpersist()
BRIDGE_DF.count()

# COMMAND ----------

main_f = spark.sql("""
  SELECT SID, COMPANY_ID
  FROM (
    SELECT DISTINCT 
      SID, 
      COMPANY_ID, 
      ROW_NUMBER() OVER (PARTITION BY SID ORDER BY COMPANY_ID) AS _NUM 
    FROM BRIDGE_DF
  )
  WHERE _NUM = 1
""")

main_f.persist(StorageLevel.DISK_ONLY)
main_f.count()

# COMMAND ----------

# LOAD
main_f.write.format('delta').mode('overwrite').save(target_file)

# COMMAND ----------

SF3.unpersist()
SF4.unpersist()
CSRS2.unpersist()
DF.unpersist()
DF_NULL.unpersist()
DF_VAL.unpersist()
DF_NULL2.unpersist()
DF_VAL2.unpersist()
DF_NULL3.unpersist()
DF_VAL3.unpersist()
DF_NULL4.unpersist()
DF_VAL4.unpersist()
DF_FIN.unpersist()
BRIDGE_DF2.unpersist()
BRIDGE_DF3.unpersist()
BRIDGE_DF4.unpersist()
BRIDGE_DF5.unpersist()
BRIDGE_DF.unpersist()

# COMMAND ----------


