# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_finance

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_finance.ar_transactions

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'ebs.ar_payment_schedules_all', prune_days)
  ar_payment_schedules_all = load_incr_dataset('ebs.ar_payment_schedules_all', '_MODIFIED', cutoff_value)
else:
  ar_payment_schedules_all = load_full_dataset('ebs.ar_payment_schedules_all')
  # VIEWS
ar_payment_schedules_all.createOrReplaceTempView('ar_payment_schedules_all')

# COMMAND ----------

# SAMPLING
if sampling:
  ra_cust_trx_line_gl_dist_all = ra_cust_trx_line_gl_dist_all.limit(10)
  ra_cust_trx_line_gl_dist_all.createOrReplaceTempView('ra_cust_trx_line_gl_dist_all')

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

CUSTOMER_LOC_USE_D = spark.sql("""
  SELECT
    HCSUA.SITE_USE_ID, 
    HCSUA.LAST_UPDATED_BY,
    HCSUA.CREATION_DATE,
    HCSUA.CREATED_BY,
    HCA.CUST_ACCOUNT_ID,
    HCSUA.SITE_USE_CODE,
    HCSUA.LOCATION,
    HL.LOCATION_ID,
    HCASA.CUST_ACCT_SITE_ID,
    HPS.PARTY_SITE_ID,
    HCASA.LAST_UPDATE_DATE,
    HCSUA.STATUS,
    HCSUA.TERRITORY_ID
  FROM EBS.HZ_CUST_ACCT_SITES_ALL HCASA
  INNER JOIN EBS.HZ_CUST_SITE_USES_ALL HCSUA ON HCSUA.CUST_ACCT_SITE_ID = HCASA.CUST_ACCT_SITE_ID
  INNER JOIN EBS.HZ_PARTY_SITES HPS ON HCASA.PARTY_SITE_ID = HPS.PARTY_SITE_ID
  INNER JOIN EBS.HZ_CUST_ACCOUNTS HCA ON HCA.CUST_ACCOUNT_ID = HCASA.CUST_ACCOUNT_ID
  INNER JOIN EBS.HZ_LOCATIONS HL ON HPS.LOCATION_ID = HL.LOCATION_ID
""")

CUSTOMER_LOC_USE_D.createOrReplaceTempView("CUSTOMER_LOC_USE_D")

# COMMAND ----------

main = spark.sql("""
SELECT
  REPLACE(
    STRING(INT(RA_CUST_TRX_LINE_GL_DIST_ALL.CREATED_BY)),
    ",",
    ""
  ) createdBy,
  RA_CUST_TRX_LINE_GL_DIST_ALL.CREATION_DATE createdOn,
  REPLACE(
    STRING(
      INT(RA_CUST_TRX_LINE_GL_DIST_ALL.LAST_UPDATED_BY)
    ),
    ",",
    ""
  ) modifiedBy,
  RA_CUST_TRX_LINE_GL_DIST_ALL.LAST_UPDATE_DATE modifiedOn,
  CURRENT_TIMESTAMP() AS insertedOn,
  CURRENT_TIMESTAMP() AS updatedOn,
  'RA_CUST_TRX_LINE_GL_DIST_ALL' || '-' || INT(
    RA_CUST_TRX_LINE_GL_DIST_ALL.CUST_TRX_LINE_GL_DIST_ID
  ) accountDocId,
  RA_CUST_TRX_LINE_GL_DIST_ALL.ACCTD_AMOUNT accountedAmount,
  AR_PAYMENT_SCHEDULES_ALL.ACTUAL_DATE_CLOSED actualDateClosed,
  AR_PAYMENT_SCHEDULES_ALL.AMOUNT_DUE_ORIGINAL arDocAmount,
  AR_PAYMENT_SCHEDULES_ALL.AMOUNT_DUE_REMAINING arRemainingAmount,
  BILL_TO.SITE_USE_CODE || '-' || REPLACE(
    STRING(INT(BILL_TO.SITE_USE_ID)),
    ",",
    ""
  ) billToAddressid,

  'COMPANY-' || REPLACE(
    STRING(INT(RA_CUSTOMER_TRX_ALL.LEGAL_ENTITY_ID)),
    ",",
    ""
  ) as companyId,
  GL_ACCOUNT.costCenterNumber as costCenterId,
  INT(RA_CUSTOMER_TRX_ALL.BILL_TO_CUSTOMER_ID) customerId,
  case
    when AR_PAYMENT_SCHEDULES_ALL.AMOUNT_DUE_ORIGINAL < 0 then 'CREDIT'
    else 'DEBIT'
  end debitCreditFlag,
  RA_CUSTOMER_TRX_ALL.INVOICE_CURRENCY_CODE docCurrency,
  AR_PAYMENT_SCHEDULES_ALL.DUE_DATE dueDate,
    REPLACE(
    STRING(
      INT(
        RA_CUST_TRX_LINE_GL_DIST_ALL.CODE_COMBINATION_ID
      )
    ),
    ",",
    ""
  ) glAccountid,
  RA_CUST_TRX_LINE_GL_DIST_ALL.GL_DATE glDate,
  RA_CUST_TRX_LINE_GL_DIST_ALL.AMOUNT glDistAmount,
  RA_CUSTOMER_TRX_ALL.INTERFACE_HEADER_CONTEXT interfaceHeaderContext,
  RA_CUSTOMER_TRX_ALL.TRX_DATE invoiceDate,
  INT(RA_CUSTOMER_TRX_ALL.CUSTOMER_TRX_ID) invoiceid,
  GL_SETS_OF_BOOKS.CURRENCY_CODE locCurrency,
  REPLACE(
    STRING(INT(RA_CUSTOMER_TRX_ALL.ORG_ID)),
    ",",
    ""
  ) owningBusinessUnitId,
  RA_CUST_TRX_LINE_GL_DIST_ALL.GL_POSTED_DATE postDate,
  INT(RA_CUSTOMER_TRX_ALL.PRIMARY_SALESREP_ID) primarySalesrepId,
  REPLACE(
    STRING(INT(GL_ACCOUNT.profitCenterNumber)),
    ",",
    ""
  ) as profitCenterId,
  RA_CUSTOMER_TRX_ALL.INTERFACE_HEADER_ATTRIBUTE1 projectNumber,
  RA_CUSTOMER_TRX_ALL.PURCHASE_ORDER purchaseOrderNumber,
  AR_PAYMENT_SCHEDULES_ALL.PAYMENT_SCHEDULE_ID referenceDocumentNumber,
  RA_CUSTOMER_TRX_ALL.TRX_NUMBER salesInvoiceNumber,
  REPLACE(
    STRING(
      INT(RA_CUST_TRX_LINE_GL_DIST_ALL.SET_OF_BOOKS_ID)
    ),
    ",",
    ""
  ) setOfBooksId,
  SHIP_TO.SITE_USE_CODE || '-' || REPLACE(
    STRING(INT(SHIP_TO.SITE_USE_ID)),
    ",",
    ""
  ) AS shipToAddressId,
  INT(RA_CUSTOMER_TRX_ALL.TERM_ID) termId,
  AR_PAYMENT_SCHEDULES_ALL.TERMS_SEQUENCE_NUMBER termSequenceNumber,
  'P' || '-' || INT(AR_PAYMENT_SCHEDULES_ALL.PAYMENT_SCHEDULE_ID) transactionDetailId,
  INT(RA_CUST_TRX_TYPES_ALL.CUST_TRX_TYPE_ID) transactionTypeid
FROM
  EBS.RA_CUST_TRX_LINE_GL_DIST_ALL
  JOIN EBS.RA_CUSTOMER_TRX_ALL ON RA_CUST_TRX_LINE_GL_DIST_ALL.CUSTOMER_TRX_ID = RA_CUSTOMER_TRX_ALL.CUSTOMER_TRX_ID
  JOIN AR_PAYMENT_SCHEDULES_ALL ON RA_CUSTOMER_TRX_ALL.CUSTOMER_TRX_ID = AR_PAYMENT_SCHEDULES_ALL.CUSTOMER_TRX_ID
  LEFT JOIN EBS.RA_CUST_TRX_TYPES_ALL ON RA_CUST_TRX_TYPES_ALL.CUST_TRX_TYPE_ID = RA_CUSTOMER_TRX_ALL.CUST_TRX_TYPE_ID
  AND RA_CUST_TRX_TYPES_ALL.ORG_ID = RA_CUSTOMER_TRX_ALL.ORG_ID
  left join GL_ACCOUNT on GL_ACCOUNT.glCombinationId = REPLACE(
    STRING(
      INT(
        RA_CUST_TRX_LINE_GL_DIST_ALL.CODE_COMBINATION_ID
      )
    ),
    ",",
    ""
  )
  left join ebs.GL_SETS_OF_BOOKS on RA_CUST_TRX_LINE_GL_DIST_ALL.SET_OF_BOOKS_ID = GL_SETS_OF_BOOKS.SET_OF_BOOKS_ID
  LEFT JOIN CUSTOMER_LOC_USE_D SHIP_TO ON RA_CUSTOMER_TRX_ALL.SHIP_TO_SITE_USE_ID = SHIP_TO.SITE_USE_ID
  LEFT JOIN CUSTOMER_LOC_USE_D BILL_TO ON nvl(
    RA_CUSTOMER_TRX_ALL.BILL_TO_SITE_USE_ID,
    AR_PAYMENT_SCHEDULES_ALL.CUSTOMER_SITE_USE_ID
  ) = BILL_TO.SITE_USE_ID
WHERE
  RA_CUSTOMER_TRX_ALL.COMPLETE_FLAG = 'Y'
  AND RA_CUST_TRX_LINE_GL_DIST_ALL.ACCOUNT_CLASS = 'REC'
  AND RA_CUST_TRX_LINE_GL_DIST_ALL.LATEST_REC_FLAG = 'Y'
  AND AR_PAYMENT_SCHEDULES_ALL.CLASS <> 'PMT'
  AND AR_PAYMENT_SCHEDULES_ALL.PAYMENT_SCHEDULE_ID <> -1
""")
main.createOrReplaceTempView('main')

# COMMAND ----------

# VALIDATE NK
valid_count_rows(main, 'transactionDetailId')

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .transform(tg_default(source_name))
  .transform(tg_finance_ar_transactions())
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

keys = (
  spark.sql("""
    SELECT   
    'P' || '-' || INT(AR_PAYMENT_SCHEDULES_ALL.PAYMENT_SCHEDULE_ID) transactionDetailId
    FROM
  ebs.AR_PAYMENT_SCHEDULES_ALL
  where
  AR_PAYMENT_SCHEDULES_ALL.CLASS <> 'PMT'
  AND AR_PAYMENT_SCHEDULES_ALL.PAYMENT_SCHEDULE_ID <> -1
  """)
)

full_keys_f = (
  keys
  .transform(attach_source_column(source = source_name))
  .transform(attach_surrogate_key(columns = 'transactionDetailId,_Source'))
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
  cutoff_value = get_incr_col_max_value(ar_payment_schedules_all,'_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 'ebs.ar_payment_schedules_all')
  update_run_datetime(run_datetime, table_name, 'ebs.ar_payment_schedules_all')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_finance
