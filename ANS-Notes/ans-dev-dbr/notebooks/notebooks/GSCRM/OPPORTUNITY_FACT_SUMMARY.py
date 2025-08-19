# Databricks notebook source
dbutils.widgets.text("target_folder", "","")
targetFolder = getArgument("target_folder")
targetFolder = '/datalake/GSCRM' if targetFolder == '' else targetFolder

dbutils.widgets.text("incremental", "","")
incremental = getArgument("incremental")
incremental = (incremental.lower() == 'true')

dbutils.widgets.text("prune_days", "","") 
pruneDays = getArgument("prune_days")
pruneDays = 30 if pruneDays == '' else pruneDays

# COMMAND ----------

tableName = 'OPPORTUNITY_FACT_SUMMARY'
keys = 'OPPORTUNITY_ID'
targetFileUrl = '/mnt/datalake/' + targetFolder + '/' + tableName + '.par'

# COMMAND ----------

print(targetFileUrl)
print(incremental)
print(pruneDays)

# COMMAND ----------

if(incremental == True):
  OPPORTUNITY_INC = spark.sql("SELECT * FROM sf.Opportunity WHERE SystemModstamp > DATE_SUB(CURRENT_DATE(), {0})".format(pruneDays))
else:
  OPPORTUNITY_INC = spark.sql("SELECT * FROM sf.Opportunity")

OPPORTUNITY_INC.createOrReplaceTempView("OPPORTUNITY_INC")

# COMMAND ----------

OPPORTUNITY = spark.sql("""
	SELECT
		o.Opportunity_ID_18__c AS OPPORTUNITY_ID,
		o.Opportunity_Number__c AS OPPORTUNITY_NUMBER,
		o.Guardian__c AS GUARDIAN,
		o.Guardian_Survey_ID__c AS GUARDIAN_SURVEY_ID,
		a.Organisation__c AS ORGANISATION,
		o.Owner_Profile_Name__c AS OPPORTUNITY_OWNER_PROFILE, 
		o.Opportunity_Owner_Name__c AS OPPORTUNITY_OWNER,
		o.Name AS OPPORTUNITY_NAME,
		o.CurrencyIsoCode AS AMOUNT_CURRENCY,
		'USD' AS AMOUNT_CONVERTED_CURRENCY,   
		DATE_FORMAT(o.CreatedDate, 'yyyy-MM-dd') AS CREATED_DATE, 
		DATE_FORMAT(o.CloseDate, 'yyyy-MM-dd') AS CLOSE_DATE,
		CONCAT(YEAR(o.CloseDate), '-', MONTH(o.CloseDate)) AS CLOSE_MONTH,
		DATE_FORMAT(o.Implementation_Date__c, 'yyyy-MM-dd') AS DATE_OF_FIRST_PO,
		o.StageName AS STAGE_NAME,
		CONCAT('Q', o.FiscalQuarter, '-', o.FiscalYear) AS FISCAL_PERIOD,
		o.Vertical__c AS VERTICAL,
		DATE_FORMAT(o.LastModifiedDate, 'yyyy-MM-dd') AS LAST_MODIFIED_DATE,
		DATE_FORMAT(o.LastActivityDate, 'yyyy-MM-dd') AS LAST_ACTIVITY,		
		CASE
		WHEN o.IsWon = 0 AND o.IsClosed = 0 THEN 'Open'
		WHEN o.IsWon = 0 AND o.IsClosed = 1 THEN 'Closed'
		WHEN o.IsWon = 1 AND o.IsClosed = 1 THEN 'Closed Won'
		END AS STATUS,
		a.Name AS ACCOUNT_NAME,
		pa.Name AS PARENT_ACCOUNT,
		CASE 
		  WHEN u.User_Region__c = 'INDCAD' THEN 'CANADA'
		  WHEN u.User_Region__c = 'INDGL' THEN 'GL'
		  WHEN u.User_Region__c = 'INDNC' THEN 'NC'
		  WHEN u.User_Region__c = 'INDNE' THEN 'NE'
		  WHEN u.User_Region__c = 'INDNC' THEN 'NC'
		  WHEN u.User_Region__c = 'INDSC' THEN 'SC'
		  WHEN u.User_Region__c = 'INDSE' THEN 'SE'
		  WHEN u.User_Region__c = 'INDWE' THEN 'WE'
		  WHEN u.User_Region__c = 'MEDCAD' THEN 'CANADA'
		  WHEN u.User_Region__c = 'MEDW' THEN 'WEST'
		  WHEN u.User_Region__c = 'MEDS' THEN 'SOUTH'
		  WHEN u.User_Region__c = 'MEDN' THEN 'NORTH'
		  WHEN u.User_Region__c = 'OLAC' THEN 'OLAC'
          WHEN o.Region3__c = 'UK - EIRE' THEN 'UK-EIRE'
		  WHEN o.Region3__c = 'CANADA' AND u.User_Region__c NOT IN('CANADA','INDCAD') THEN u.User_Region__c  
		  ELSE UPPER(COALESCE(o.Region3__c, u.User_Region__c))
		END AS SUB_REGION,
		CASE
		WHEN o.CurrencyIsoCode = 'USD' THEN 1
		WHEN o.CreatedDate > CURRENT_DATE() OR o.CloseDate > CURRENT_DATE() THEN (
			SELECT FIRST(ConversionRate)
			FROM sf.CurrencyType
			WHERE 
			  IsoCode = o.CurrencyIsoCode 
			  AND CURRENT_DATE() >= EFFECTIVE_START_DATE AND CURRENT_DATE() <= EFFECTIVE_END_DATE
		)
		ELSE COALESCE(ct.ConversionRate, ct2.ConversionRate)
		END CONVERSION_RATE,
		r.DeveloperName AS RECORD_TYPE
	FROM OPPORTUNITY_INC o
	LEFT JOIN sf.Account a ON o.AccountID = a.Id
	LEFT JOIN sf.Account pa ON a.ParentId = pa.Id
	LEFT JOIN sf.User u ON o.OwnerId = u.Id
	LEFT JOIN sf.RecordType r ON r.Id = o.RecordTypeId
	LEFT JOIN sf.CurrencyType ct ON 
		o.CurrencyIsoCode = ct.IsoCode 
		AND o.IsWon = 0 AND o.IsClosed = 0
		AND CAST(o.CreatedDate AS DATE) >= ct.EFFECTIVE_START_DATE 
		AND CAST(o.CreatedDate AS DATE) <= ct.EFFECTIVE_END_DATE
	LEFT JOIN sf.CurrencyType ct2 ON 
		o.CurrencyIsoCode = ct2.IsoCode 
		AND (o.IsWon = 1 OR o.IsClosed = 1) 
		AND CAST(o.CloseDate AS DATE) >= ct2.EFFECTIVE_START_DATE 
		AND CAST(o.CloseDate AS DATE) <= ct2.EFFECTIVE_END_DATE
	WHERE o.DELETE_FLG = 'N'
""")

OPPORTUNITY.createOrReplaceTempView("OPPORTUNITY")
display(OPPORTUNITY)

# COMMAND ----------

OPPORTUNITY_LINE = spark.sql("""
  SELECT
    oli.OpportunityId AS OPPORTUNITY_ID,
    SUM(oli.Subtotal) AS AMOUNT,
    SUM(oli.Subtotal_USD) AS AMOUNT_CONVERTED
  FROM sf.OpportunityLineItem oli
  JOIN OPPORTUNITY_INC o ON oli.OpportunityId = o.Id
  WHERE oli.DELETE_FLG = 'N'
  GROUP BY oli.OpportunityId
""")

OPPORTUNITY_LINE.createOrReplaceTempView("OPPORTUNITY_LINE")
display(OPPORTUNITY_LINE)

# COMMAND ----------

df = spark.sql("""
  SELECT
    o.*,
    oli.AMOUNT, 
    oli.AMOUNT / o.CONVERSION_RATE AS AMOUNT_CONVERTED 		
  FROM OPPORTUNITY o
  JOIN OPPORTUNITY_LINE oli ON o.OPPORTUNITY_ID = oli.OPPORTUNITY_ID
  WHERE
    o.CLOSE_DATE IS NULL OR o.CLOSE_DATE >= '2020-06-31' 
    OR o.STATUS = 'Open'
""")

df.createOrReplaceTempView("df")
display(df)

# COMMAND ----------

try:
  dbutils.fs.ls(targetFileUrl)
  fileExists = True
except Exception as e:
  fileExists = False

if not fileExists:
  print("Creating new file")
  df.write.format("delta").save(targetFileUrl)

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS GSCRM")
spark.sql("USE GSCRM")
spark.sql("CREATE TABLE IF NOT EXISTS {0} USING DELTA LOCATION '{1}'".format(tableName, targetFileUrl))
spark.sql("ALTER TABLE {0} SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)".format(tableName))
spark.sql("ANALYZE TABLE {0} COMPUTE STATISTICS NOSCAN".format(tableName))
spark.sql("OPTIMIZE {0}".format(tableName))

# COMMAND ----------

keysArray = [key.strip() for key in keys.split(',')]
join = ' AND '.join(["{0}.{1} = df.{1}".format(tableName, key) for key in keysArray])  
query = "MERGE INTO {0} USING df ON {1} WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *".format(tableName, join)
print(query)
spark.sql(query)

# COMMAND ----------


