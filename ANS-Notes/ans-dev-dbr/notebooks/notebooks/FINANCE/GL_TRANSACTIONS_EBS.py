# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_finance

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_finance.gl_transactions

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'ebs.gl_je_lines', prune_days)
  gl_je_lines = load_incr_dataset('ebs.gl_je_lines', '_MODIFIED', cutoff_value)
else:
  gl_je_lines = load_full_dataset('ebs.gl_je_lines')
  # VIEWS
gl_je_lines.createOrReplaceTempView('gl_je_lines')

# COMMAND ----------

# SAMPLING
if sampling:
  gl_je_lines = gl_je_lines.limit(10)
  gl_je_lines.createOrReplaceTempView('gl_je_lines')

# COMMAND ----------

GL_ACCOUNT = spark.sql("""
SELECT
     REPLACE(STRING(INT(GL_CODE_COMBINATIONS.CHART_OF_ACCOUNTS_ID)), ",", "" ) as chartOfAccounts
    , REPLACE(STRING(INT(FND_ID_FLEX_SEGMENTS3.FLEX_VALUE_SET_ID)), ",", "" ) as costCenterAttribute
    , FND_SEGMENT_ATTRIBUTE_VALUES1.APPLICATION_COLUMN_NAME AS costCenterDescription
    , CASE  
          WHEN FND_SEGMENT_ATTRIBUTE_VALUES1.APPLICATION_COLUMN_NAME = 'SEGMENT1'
              THEN GL_CODE_COMBINATIONS.SEGMENT1
          WHEN FND_SEGMENT_ATTRIBUTE_VALUES1.APPLICATION_COLUMN_NAME = 'SEGMENT2'
              THEN GL_CODE_COMBINATIONS.SEGMENT2
          WHEN FND_SEGMENT_ATTRIBUTE_VALUES1.APPLICATION_COLUMN_NAME = 'SEGMENT3'
              THEN GL_CODE_COMBINATIONS.SEGMENT3
          WHEN FND_SEGMENT_ATTRIBUTE_VALUES1.APPLICATION_COLUMN_NAME = 'SEGMENT4'
              THEN GL_CODE_COMBINATIONS.SEGMENT4
          WHEN FND_SEGMENT_ATTRIBUTE_VALUES1.APPLICATION_COLUMN_NAME = 'SEGMENT5'
              THEN GL_CODE_COMBINATIONS.SEGMENT5
          WHEN FND_SEGMENT_ATTRIBUTE_VALUES1.APPLICATION_COLUMN_NAME = 'SEGMENT6'
              THEN GL_CODE_COMBINATIONS.SEGMENT6
      END AS costCenterNumber
    , FND_SEGMENT_ATTRIBUTE_VALUES.APPLICATION_COLUMN_NAME AS glAccountDescription
    , CASE  
          WHEN FND_SEGMENT_ATTRIBUTE_VALUES.APPLICATION_COLUMN_NAME = 'SEGMENT1'
              THEN GL_CODE_COMBINATIONS.SEGMENT1
          WHEN FND_SEGMENT_ATTRIBUTE_VALUES.APPLICATION_COLUMN_NAME = 'SEGMENT2'
              THEN GL_CODE_COMBINATIONS.SEGMENT2
          WHEN FND_SEGMENT_ATTRIBUTE_VALUES.APPLICATION_COLUMN_NAME = 'SEGMENT3'
              THEN GL_CODE_COMBINATIONS.SEGMENT3
          WHEN FND_SEGMENT_ATTRIBUTE_VALUES.APPLICATION_COLUMN_NAME = 'SEGMENT4'
              THEN GL_CODE_COMBINATIONS.SEGMENT4
          WHEN FND_SEGMENT_ATTRIBUTE_VALUES.APPLICATION_COLUMN_NAME = 'SEGMENT5'
              THEN GL_CODE_COMBINATIONS.SEGMENT5
          WHEN FND_SEGMENT_ATTRIBUTE_VALUES.APPLICATION_COLUMN_NAME = 'SEGMENT6'
              THEN GL_CODE_COMBINATIONS.SEGMENT6
      END AS glAccountNumber
    , REPLACE(STRING(INT(GL_CODE_COMBINATIONS.CODE_COMBINATION_ID)), ",", "" ) AS glCombinationId
    , REPLACE(STRING(INT(FND_ID_FLEX_SEGMENTS2.FLEX_VALUE_SET_ID)), ",", "" ) AS profitCenterAttribute
    , FND_SEGMENT_ATTRIBUTE_VALUES2.APPLICATION_COLUMN_NAME  AS profitCenterDescription
     , CASE  
          WHEN FND_SEGMENT_ATTRIBUTE_VALUES2.APPLICATION_COLUMN_NAME = 'SEGMENT1'
              THEN GL_CODE_COMBINATIONS.SEGMENT1
          WHEN FND_SEGMENT_ATTRIBUTE_VALUES2.APPLICATION_COLUMN_NAME = 'SEGMENT2'
              THEN GL_CODE_COMBINATIONS.SEGMENT2
          WHEN FND_SEGMENT_ATTRIBUTE_VALUES2.APPLICATION_COLUMN_NAME = 'SEGMENT3'
              THEN GL_CODE_COMBINATIONS.SEGMENT3
          WHEN FND_SEGMENT_ATTRIBUTE_VALUES2.APPLICATION_COLUMN_NAME = 'SEGMENT4'
              THEN GL_CODE_COMBINATIONS.SEGMENT4
          WHEN FND_SEGMENT_ATTRIBUTE_VALUES2.APPLICATION_COLUMN_NAME = 'SEGMENT5'
              THEN GL_CODE_COMBINATIONS.SEGMENT5
          WHEN FND_SEGMENT_ATTRIBUTE_VALUES2.APPLICATION_COLUMN_NAME = 'SEGMENT6'
              THEN GL_CODE_COMBINATIONS.SEGMENT6
      END AS profitCenterNumber
       , (select 
            MAX(REPLACE(STRING(INT(SEG4_ATTRIB)), ",", "" )) 
         from smartsheets.W_GLACCT_SEG_CONFIG_TMP 
         where REPLACE(STRING(INT(CHARTFIELD_GROUP_ID)), ",", "" ) = REPLACE(STRING(INT(GL_CODE_COMBINATIONS.CHART_OF_ACCOUNTS_ID)), ",", "" )
         ) segment4Attribute
    ,  GL_CODE_COMBINATIONS.SEGMENT4 AS segment4Code
     , (select 
            MAX(REPLACE(STRING(INT(SEG7_ATTRIB)), ",", "" )) 
         from smartsheets.W_GLACCT_SEG_CONFIG_TMP 
         where REPLACE(STRING(INT(CHARTFIELD_GROUP_ID)), ",", "" ) = REPLACE(STRING(INT(GL_CODE_COMBINATIONS.CHART_OF_ACCOUNTS_ID)), ",", "" )
         ) segment7Attribute
    , GL_CODE_COMBINATIONS.SEGMENT7 AS segment7Code
    , (select 
            MAX(REPLACE(STRING(INT(SEG6_ATTRIB)), ",", "" )) 
         from smartsheets.W_GLACCT_SEG_CONFIG_TMP 
         where REPLACE(STRING(INT(CHARTFIELD_GROUP_ID)), ",", "" ) = REPLACE(STRING(INT(GL_CODE_COMBINATIONS.CHART_OF_ACCOUNTS_ID)), ",", "" )
         ) segment6Attribute
    , GL_CODE_COMBINATIONS.SEGMENT6 AS segment6Code
     , (select 
            MAX(REPLACE(STRING(INT(SEG1_ATTRIB)), ",", "" )) 
         from smartsheets.W_GLACCT_SEG_CONFIG_TMP 
         where REPLACE(STRING(INT(CHARTFIELD_GROUP_ID)), ",", "" ) = REPLACE(STRING(INT(GL_CODE_COMBINATIONS.CHART_OF_ACCOUNTS_ID)), ",", "" )
         ) segment1Attribute
    , GL_CODE_COMBINATIONS.SEGMENT1 AS segment1Code
     , (select 
            MAX(REPLACE(STRING(INT(SEG5_ATTRIB)), ",", "" )) 
         from smartsheets.W_GLACCT_SEG_CONFIG_TMP 
         where REPLACE(STRING(INT(CHARTFIELD_GROUP_ID)), ",", "" ) = REPLACE(STRING(INT(GL_CODE_COMBINATIONS.CHART_OF_ACCOUNTS_ID)), ",", "" )
         ) segment5Attribute
    , GL_CODE_COMBINATIONS.SEGMENT5 AS segment5Code
   
    
FROM
    ebs.gl_code_combinations
    JOIN ebs.fnd_segment_attribute_values
      ON fnd_segment_attribute_values.id_flex_num = gl_code_combinations.chart_of_accounts_id
        AND fnd_segment_attribute_values.application_id = 101
        AND fnd_segment_attribute_values.id_flex_code = 'GL#'
        AND fnd_segment_attribute_values.segment_attribute_type = 'GL_ACCOUNT'
        AND fnd_segment_attribute_values.attribute_value = 'Y'
    left join ebs.fnd_segment_attribute_values fnd_segment_attribute_values1
      ON fnd_segment_attribute_values1.id_flex_num  = gl_code_combinations.chart_of_accounts_id
        AND fnd_segment_attribute_values1.application_id  = 101
        AND fnd_segment_attribute_values1.id_flex_code  = 'GL#'
        AND fnd_segment_attribute_values1.segment_attribute_type = 'FA_COST_CTR'
        AND fnd_segment_attribute_values1.attribute_value = 'Y'
    join ebs.fnd_segment_attribute_values fnd_segment_attribute_values2
      ON fnd_segment_attribute_values2.id_flex_num = gl_code_combinations.chart_of_accounts_id
        AND fnd_segment_attribute_values2.application_id = 101
        AND fnd_segment_attribute_values2.id_flex_code = 'GL#'
        AND fnd_segment_attribute_values2.segment_attribute_type = 'GL_BALANCING'
        AND fnd_segment_attribute_values2.attribute_value = 'Y'
    JOIN ebs.fnd_id_flex_segments  -- GL Accounts
      ON fnd_id_flex_segments.id_flex_num = gl_code_combinations.chart_of_accounts_id
        AND fnd_id_flex_segments.id_flex_code = 'GL#'
        AND fnd_id_flex_segments.application_id = 101
        AND fnd_id_flex_segments.application_column_name = fnd_segment_attribute_values.application_column_name   
    JOIN ebs.fnd_id_flex_segments         fnd_id_flex_segments2 -- profit center
      ON fnd_id_flex_segments2.id_flex_num = gl_code_combinations.chart_of_accounts_id
        AND fnd_id_flex_segments2.id_flex_code = 'GL#'
        AND fnd_id_flex_segments2.application_id = 101
        AND fnd_id_flex_segments2.application_column_name = fnd_segment_attribute_values2.application_column_name 
    JOIN ebs.fnd_id_flex_segments         fnd_id_flex_segments3 -- cost center
      ON fnd_id_flex_segments3.id_flex_num = gl_code_combinations.chart_of_accounts_id
        AND fnd_id_flex_segments3.id_flex_code = 'GL#'
        AND fnd_id_flex_segments3.application_id = 101  
        AND fnd_id_flex_segments3.application_column_name = fnd_segment_attribute_values1.application_column_name
    join ebs.fnd_id_flex_structures
      ON fnd_id_flex_structures.id_flex_num = gl_code_combinations.chart_of_accounts_id
        AND fnd_id_flex_structures.id_flex_code = 'GL#'
        AND fnd_id_flex_structures.application_id = 101
        AND fnd_id_flex_structures.enabled_flag = 'Y'
        """)
GL_ACCOUNT.createOrReplaceTempView("GL_ACCOUNT")

# COMMAND ----------

W_EXCH_RATE_G = spark.sql("""
  SELECT 
    GL_DAILY_RATES.FROM_CURRENCY, 
    GL_DAILY_RATES.TO_CURRENCY, 
    GL_DAILY_RATES.CONVERSION_TYPE, 
    CAST(DATE_FORMAT(GL_DAILY_RATES.CONVERSION_DATE,"yyyyMMdd") AS INT) CONVERSION_DATE,
    GL_DAILY_RATES.FROM_CURRENCY FROM_CURCY_CD,
    GL_DAILY_RATES.TO_CURRENCY TO_CURCY_CD,
    GL_DAILY_RATES.CONVERSION_TYPE RATE_TYPE,
    GL_DAILY_RATES.CONVERSION_DATE START_DT,
    DATE_ADD(GL_DAILY_RATES.CONVERSION_DATE,1) END_DT,
    GL_DAILY_RATES.CONVERSION_RATE EXCH_RATE,
    GL_DAILY_RATES.CONVERSION_DATE EXCH_DT
  FROM 
    EBS.GL_DAILY_RATES
    where upper(CONVERSION_TYPE) = 'CORPORATE'
    and FROM_CURRENCY = 'USD'
""")
W_EXCH_RATE_G.createOrReplaceTempView("W_EXCH_RATE_G")

# COMMAND ----------

COMPANY = spark.sql("""SELECT
LCD.OBJECT_ID LEDGER_ID,
LCD2.OBJECT_ID LEGAL_ENTITY_ID,
LEB.FLEX_VALUE_SET_ID,
LEB.FLEX_SEGMENT_VALUE
FROM
EBS.GL_LEDGER_CONFIG_DETAILS LCD,
EBS.GL_LEDGER_CONFIG_DETAILS LCD2,
EBS.GL_LEGAL_ENTITIES_BSVS LEB
WHERE
LCD.OBJECT_TYPE_CODE IN ('PRIMARY', 'SECONDARY') AND
LCD.SETUP_STEP_CODE = 'NONE' AND
LCD2.OBJECT_TYPE_CODE = 'LEGAL_ENTITY' AND
LCD.CONFIGURATION_ID = LCD2.CONFIGURATION_ID AND
LCD2.OBJECT_ID = LEB.LEGAL_ENTITY_ID
""")
COMPANY.createOrReplaceTempView("COMPANY")

# COMMAND ----------

main = spark.sql("""
SELECT
  REPLACE(
    STRING(INT(JEL.CREATED_BY)),
    ",",
    ""
  ) as createdBy,
  JEL.CREATION_DATE as createdOn,
  REPLACE(
    STRING(INT(JEL.LAST_UPDATED_BY)),
    ",",
    ""
  ) AS modifiedBy,
  JEL.LAST_UPDATE_DATE AS modifiedOn,
  CURRENT_TIMESTAMP() as insertedOn,
  CURRENT_TIMESTAMP() as updatedOn,
  JEL.JE_LINE_NUM as accountingDocumentItem,
  JEB.NAME as accountingDocumentNumber,
  nvl(CASE WHEN JEL.ENTERED_DR > 1000000000 THEN 0 ELSE JEL.ENTERED_DR END, 0) - nvl(CASE WHEN JEL.ENTERED_CR > 1000000000 THEN 0 ELSE JEL.ENTERED_CR END, 0) as amountDocumentCurrency,
  nvl(CASE WHEN JEL.ACCOUNTED_DR > 1000000000 THEN 0 ELSE JEL.ACCOUNTED_DR END, 0) - nvl(CASE WHEN JEL.ACCOUNTED_CR > 1000000000 THEN 0 ELSE JEL.ACCOUNTED_CR END, 0) as amountLocalCurrency,
  'COMPANY-' ||  REPLACE(
    STRING(INT(COMPANY.LEGAL_ENTITY_ID)),
    ",",
    ""
 ) as companyId,
  GL_ACCOUNT.costCenterAttribute ||'-'|| GL_ACCOUNT.costCenterNumber AS costCenterId,
  case
    when nvl(JEL.ENTERED_DR, 0) - nvl(JEL.ENTERED_CR, 0) < 0 then 'CREDIT'
    else 'DEBIT'
  end as debitCreditIndicator,
  GL_ACCOUNT.segment4Attribute ||'-'|| GL_ACCOUNT.segment4Code AS divisionId,
  JEH.CURRENCY_CODE as documentCurrencyCode,
  'POSTED' as documentStatus,
  'OTHERS' as documentTypeId,
  PRDS.END_DATE as enterpriseEndDate,
  EXCHANGE_RATE_AGG.exchangeRate as exchangeRateAverageUsd,
  W_EXCH_RATE_G.EXCH_RATE  exchangeRateUsd,
  GL_ACCOUNT.segment7Attribute ||'-'|| GL_ACCOUNT.segment7Code as futureId,
   REPLACE(
    STRING(INT(JEL.CODE_COMBINATION_ID)),
    ",",
    ""
  ) as glAccountId,
  JEH.JE_CATEGORY as glJournalCategory,
  INT(JEL.JE_HEADER_ID) || '-' || INT(JEL.JE_LINE_NUM) AS glJournalId,
  GL_ACCOUNT.segment6Attribute ||'-'|| GL_ACCOUNT.segment6Code as interCompanyId,
   REPLACE(
    STRING(INT(JEL.LEDGER_ID)),
    ",",
    ""
  ) as ledgerId,
  GL_ACCOUNT.segment1Attribute ||'-'|| GL_ACCOUNT.segment1Code AS legalEntityId,
  JEL.DESCRIPTION lineDescription,
  JEH.NAME as lineItemText,
  GSOB.CURRENCY_CODE as localCurrencyCode,
  CASE WHEN JEH.CURRENCY_CODE = GSOB.CURRENCY_CODE THEN 1 ELSE GL_DAILY_RATES.CONVERSION_RATE END  AS localExchangeRate,
  JEH.POSTED_DATE AS postedOnDate,
  GL_ACCOUNT.profitCenterAttribute ||'-'|| GL_ACCOUNT.profitCenterNumber AS profitCenterId,
  JEH.JE_SOURCE as referenceDocumentNumber,
  GL_ACCOUNT.segment5Attribute ||'-'|| GL_ACCOUNT.segment5Code as regionId,
  JEH.POSTED_DATE as transactionDate
FROM
  GL_JE_LINES JEL
  JOIN EBS.GL_JE_HEADERS JEH ON JEL.JE_HEADER_ID = JEH.JE_HEADER_ID
  JOIN EBS.GL_LEDGERS GL ON JEL.LEDGER_ID = GL.LEDGER_ID
  JOIN EBS.GL_PERIOD_STATUSES PRDS ON JEL.PERIOD_NAME = PRDS.PERIOD_NAME  AND JEL.LEDGER_ID  = PRDS.SET_OF_BOOKS_ID 
  LEFT JOIN EBS.GL_JE_BATCHES JEB ON JEH.JE_BATCH_ID = JEB.JE_BATCH_ID
  LEFT JOIN GL_ACCOUNT on GL_ACCOUNT.glCombinationId = JEL.CODE_COMBINATION_ID
 LEFT JOIN COMPANY ON GL_ACCOUNT.profitCenterAttribute = COMPANY.FLEX_VALUE_SET_ID
 AND GL_ACCOUNT.profitCenterNumber = COMPANY.FLEX_SEGMENT_VALUE
 AND  JEL.LEDGER_ID = COMPANY.LEDGER_ID
 LEFT JOIN  EBS.GL_SETS_OF_BOOKS GSOB ON  COMPANY.LEDGER_ID = GSOB.SET_OF_BOOKS_ID
 LEFT JOIN EBS.GL_DAILY_RATES ON JEH.CURRENCY_CODE = GL_DAILY_RATES.FROM_CURRENCY
      AND GSOB.CURRENCY_CODE = GL_DAILY_RATES.TO_CURRENCY
      AND date_format(GL_DAILY_RATES.CONVERSION_DATE,'yyyyMMdd') = date_format(JEH.POSTED_DATE,'yyyyMMdd')
       AND UPPER(GL_DAILY_RATES.CONVERSION_TYPE) = 'CORPORATE'
 LEFT  JOIN S_CORE.EXCHANGE_RATE_AGG ON 
      'AVERAGE' || '-' || case when length(trim(JEH.CURRENCY_CODE)) = 0 then 'USD' else  trim(JEH.CURRENCY_CODE) end || '-USD-' || date_format(JEH.POSTED_DATE, 'yyyy-MM') || '-01-QV' = EXCHANGE_RATE_AGG.fxID
 LEFT JOIN W_EXCH_RATE_G ON 
  JEH.CURRENCY_CODE = W_EXCH_RATE_G.TO_CURCY_CD
  AND JEH.POSTED_DATE >= W_EXCH_RATE_G.START_DT
  AND JEH.POSTED_DATE < W_EXCH_RATE_G.END_DT
  
WHERE
  JEH.ACTUAL_FLAG = 'A'
  AND JEH.CURRENCY_CODE <> 'STAT'
  AND JEB.STATUS = 'P' 
  AND PRDS.APPLICATION_ID  = 101 
 
 -- AND INT(JEL.JE_HEADER_ID) || '-' || INT(JEL.JE_LINE_NUM) = '7034-3'
""")
main.createOrReplaceTempView('main')

# COMMAND ----------

# VALIDATE NK
valid_count_rows(main, 'glJournalId')

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .transform(tg_default(source_name))
  .transform(tg_finance_gl_transactions())
  .transform(apply_schema(schema))  
  .transform(attach_unknown_record)
  .select(columns)
  .transform(sort_columns)
)

main_f.cache()
main_f.display()

# COMMAND ----------

# VALIDATE DATA
valid_count_rows(main_f, '_ID')

# COMMAND ----------

# DATA QUALITY
show_null_values(main_f)

# COMMAND ----------

# PERSIST DATA
options = {'target_storage': target_storage, 'target_container': target_container}
merge_to_delta(main_f, table_name, target_folder, overwrite, options = options)

# COMMAND ----------

# HANDLE DELETE
full_keys_f =  (
  spark.sql("""SELECT   
INT(JEL.JE_HEADER_ID) || '-' || INT(JEL.JE_LINE_NUM) AS glJournalId
    FROM
  ebs.GL_JE_LINES JEL

     """)
.transform(attach_source_column(source = source_name))
.transform(attach_surrogate_key(columns = 'glJournalId,_Source'))
.select('_ID')
.transform(add_unknown_ID())
 )
    
apply_soft_delete(full_keys_f, table_name, key_columns = '_ID')

# COMMAND ----------

# UPDATE FK
update_foreign_key(table_name, 'createdBy,_SOURCE', 'createdBy_ID', 'edm.user')
update_foreign_key(table_name, 'modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user')


# COMMAND ----------

# UPDATE CUTOFF VALUE
if not test_run:
  cutoff_value = get_incr_col_max_value(gl_je_lines,'_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 'ebs.gl_je_lines')
  update_run_datetime(run_datetime, table_name, 'ebs.gl_je_lines')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_finance
