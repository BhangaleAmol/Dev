# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_finance

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_finance.ap_transactions

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'ebs.ap_invoice_distributions_all', prune_days)
  ap_invoice_distributions_all = load_incr_dataset('ebs.ap_invoice_distributions_all', '_MODIFIED', cutoff_value)
else:
  ap_invoice_distributions_all = load_full_dataset('ebs.ap_invoice_distributions_all')
  # VIEWS
ap_invoice_distributions_all.createOrReplaceTempView('ap_invoice_distributions_all')

# COMMAND ----------

# SAMPLING
if sampling:
  ap_invoice_distributions_all = ap_invoice_distributions_all.limit(10)
  ap_invoice_distributions_all.createOrReplaceTempView('ap_invoice_distributions_all')

# COMMAND ----------

GL_LINKAGE = spark.sql("""
SELECT DISTINCT
  REPLACE(STRING(INT(JHEADER.CREATED_BY)), ",", "") as createdBy,
  JHEADER.CREATION_DATE as createdOn,
  REPLACE(STRING(INT(JHEADER.LAST_UPDATED_BY)), ",", "") AS modifiedBy,
  JHEADER.LAST_UPDATE_DATE AS modifiedOn,
  CURRENT_TIMESTAMP() as insertedOn,
  CURRENT_TIMESTAMP() as updatedOn,
  CASE
    WHEN DLINK.SOURCE_DISTRIBUTION_TYPE = 'AP_INV_DIST' THEN 'AP'
    WHEN DLINK.SOURCE_DISTRIBUTION_TYPE = 'AP_PMT_DIST' THEN 'AP'
    WHEN DLINK.SOURCE_DISTRIBUTION_TYPE = 'AP_PREPAY' THEN 'AP'
    WHEN DLINK.SOURCE_DISTRIBUTION_TYPE = 'AR_DISTRIBUTIONS_ALL' THEN 'AR'
    WHEN DLINK.SOURCE_DISTRIBUTION_TYPE = 'MTL_TRANSACTION_ACCOUNTS' THEN 'COGS'
    WHEN DLINK.SOURCE_DISTRIBUTION_TYPE = 'RA_CUST_TRX_LINE_GL_DIST_ALL' THEN 'AR'
  END AS distributionSource,
  REPLACE(
    STRING(INT(AELINE.CODE_COMBINATION_ID)),
    ",",
    ""
  ) AS glAccountId,
  JBATCH.NAME AS jeBatchName,
  JHEADER.NAME AS jeHeaderName,
  GLIMPREF.JE_LINE_NUM AS jeLineNum,
  INT(GLIMPREF.JE_HEADER_ID) || '-' || INT(GLIMPREF.JE_LINE_NUM) glJournalId,
  REPLACE(STRING(INT(T.LEDGER_ID)), ",", "") ledgerId,
  CASE
    WHEN T.LEDGER_CATEGORY_CODE = 'PRIMARY' THEN 'P'
    WHEN T.LEDGER_CATEGORY_CODE = 'SECONDARY' THEN 'S'
    WHEN T.LEDGER_CATEGORY_CODE = 'ALC' THEN 'R'
  END ledgerType,
  PER.END_DATE postedOnDt,
  AELINE.AE_HEADER_ID || '-' || AELINE.AE_LINE_NUM slaTrxId,
  DLINK.SOURCE_DISTRIBUTION_TYPE || CASE
    WHEN DLINK.SOURCE_DISTRIBUTION_TYPE = 'AP_INV_DIST' THEN (
      CASE
        WHEN AELINE.ACCOUNTING_CLASS_CODE = 'LIABILITY' THEN '-LIABILITY'
        ELSE '-EXPENSE'
      END
    )
    ELSE (
      CASE
        WHEN DLINK.SOURCE_DISTRIBUTION_TYPE = 'AP_PMT_DIST' THEN '-' || AELINE.ACCOUNTING_CLASS_CODE
      END
    )
  END || '-' || INT(DLINK.SOURCE_DISTRIBUTION_ID_NUM_1)  AS sourceDistributionId
FROM
  ebs.GL_LEDGERS T,
  ebs.GL_PERIODS PER,
  ebs.GL_JE_HEADERS JHEADER,
  ebs.GL_IMPORT_REFERENCES GLIMPREF,
  ebs.XLA_AE_LINES AELINE,
  ebs.XLA_DISTRIBUTION_LINKS DLINK,
  ebs.GL_JE_BATCHES JBATCH
WHERE
  AELINE.GL_SL_LINK_TABLE = GLIMPREF.GL_SL_LINK_TABLE
  AND AELINE.GL_SL_LINK_ID = GLIMPREF.GL_SL_LINK_ID
  AND AELINE.AE_HEADER_ID = DLINK.AE_HEADER_ID
  AND AELINE.AE_LINE_NUM = DLINK.AE_LINE_NUM
  AND GLIMPREF.JE_HEADER_ID = JHEADER.JE_HEADER_ID
  AND JHEADER.JE_BATCH_ID = JBATCH.JE_BATCH_ID
  AND JHEADER.LEDGER_ID = T.LEDGER_ID
  AND JHEADER.STATUS = 'P'
  AND T.PERIOD_SET_NAME = PER.PERIOD_SET_NAME
  AND JHEADER.PERIOD_NAME = PER.PERIOD_NAME
  AND DECODE ('N', 'Y', T.LEDGER_ID, 1) IN (1)
  AND DECODE ('N', 'Y', T.LEDGER_CATEGORY_CODE, 'NONE') IN ('NONE')
  AND DLINK.SOURCE_DISTRIBUTION_TYPE IN ('AP_INV_DIST')
  AND DLINK.APPLICATION_ID = 200
  AND AELINE.APPLICATION_ID = 200
""")
GL_LINKAGE.createOrReplaceTempView("GL_LINKAGE")

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

main_expense = spark.sql("""
--EXPENSE
SELECT
  REPLACE(
    STRING(INT(AP_INVOICE_DISTRIBUTIONS_ALL.CREATED_BY)),
    ",",
    ""
  ) as createdBy,
  AP_INVOICE_DISTRIBUTIONS_ALL.CREATION_DATE as createdOn,
  REPLACE(
    STRING(
      INT(AP_INVOICE_DISTRIBUTIONS_ALL.LAST_UPDATED_BY)
    ),
    ",",
    ""
  ) AS modifiedBy,
  AP_INVOICE_DISTRIBUTIONS_ALL.LAST_UPDATE_DATE AS modifiedOn,
  CURRENT_TIMESTAMP() as insertedOn,
  CURRENT_TIMESTAMP() as updatedOn,
  'AP_INV_DIST-EXPENSE' || '-' || INT(INVOICE_DISTRIBUTION_ID) accountDocId,
  AP_INVOICE_DISTRIBUTIONS_ALL.BASE_AMOUNT as baseAmount,
  'COMPANY-' || REPLACE(
    STRING(INT(AP_INVOICES_ALL.LEGAL_ENTITY_ID)),
    ",",
    ""
  ) as companyId,
  GL_ACCOUNT.costCenterNumber as costCenterId,
  AP_INVOICES_ALL.INVOICE_CURRENCY_CODE as currency,
  AP_INVOICES_ALL.INVOICE_DATE as dateInvoiced,
  CASE
    WHEN -1 *(AP_INVOICE_DISTRIBUTIONS_ALL.AMOUNT) < 0 then 'CREDIT'
    else 'DEBIT'
  end as debitCreditFlag,
  AP_INVOICE_DISTRIBUTIONS_ALL.DESCRIPTION as description,
  AP_INVOICE_DISTRIBUTIONS_ALL.DISTRIBUTION_LINE_NUMBER as distributionLineNumber,
  'EXPENSE' as distributionType,
  case
    when (
      AP_INVOICE_DISTRIBUTIONS_ALL.UNIT_PRICE * AP_INVOICE_DISTRIBUTIONS_ALL.QUANTITY_INVOICED
    ) is null then -1 * AP_INVOICE_DISTRIBUTIONS_ALL.AMOUNT
    else (
      AP_INVOICE_DISTRIBUTIONS_ALL.UNIT_PRICE * AP_INVOICE_DISTRIBUTIONS_ALL.QUANTITY_INVOICED*-1
    )
  end as documentAmount,
  AP_INVOICES_ALL.SOURCE as documentSource,
  'ACCT_DOC_STATUS-' || 'OPEN-UNPOSTED' as documentStatus,
  'ACCT_DOC' || '-' || 'PAYABLES' || '-' || 'DISTRIBUTION' || '-' || 'INVDIST-' || AP_INVOICE_DISTRIBUTIONS_ALL.LINE_TYPE_LOOKUP_CODE as documentTypeId,
  null as dueDate,
  NVL(case when AP_INVOICES_ALL.INVOICE_CURRENCY_CODE = GL_SETS_OF_BOOKS.CURRENCY_CODE THEN 1 ELSE AP_INVOICES_ALL.EXCHANGE_RATE END , 1) as exchangeRate,
  EXCHANGE_RATE_AGG.exchangeRate exchangeRateUSD,
  REPLACE(
    STRING(
      INT(
        AP_INVOICE_DISTRIBUTIONS_ALL.DIST_CODE_COMBINATION_ID
      )
    ),
    ",",
    ""
  ) as glAccountId,
  INT(AP_INVOICES_ALL.SET_OF_BOOKS_ID) || '-' || INT(
    AP_INVOICE_DISTRIBUTIONS_ALL.DIST_CODE_COMBINATION_ID
  ) || '-' || INT(AP_INVOICES_ALL.ORG_ID) as glBalanceId,
  null as glJournalId,
  AP_INVOICES_ALL.Invoice_Amount invoiceHeaderAmount,
  REPLACE(
    STRING(INT(AP_INVOICES_ALL.INVOICE_ID)),
    ",",
    ""
  ) as invoiceId,
  AP_INVOICE_DISTRIBUTIONS_ALL.INVOICE_LINE_NUMBER as invoiceLineNumber,
  AP_INVOICES_ALL.INVOICE_RECEIVED_DATE as invoiceReceivedDate,
  PO_LINES_ALL.ITEM_ID as itemId,
  REPLACE(
    STRING(INT(AP_INVOICES_ALL.SET_OF_BOOKS_ID)),
    ",",
    ""
  ) as ledgerId,
 nvl(CASE
    WHEN AP_INVOICE_DISTRIBUTIONS_ALL.BASE_AMOUNT IS NULL THEN (
      CASE
        WHEN (
          NVL(case
            when AP_INVOICES_ALL.INVOICE_CURRENCY_CODE = GL_SETS_OF_BOOKS.CURRENCY_CODE THEN 1
            ELSE AP_INVOICES_ALL.EXCHANGE_RATE
          END,1)
        ) IS NULL
        AND (
          AP_INVOICE_DISTRIBUTIONS_ALL.UNIT_PRICE * AP_INVOICE_DISTRIBUTIONS_ALL.QUANTITY_INVOICED
        ) IS NULL THEN (
          AP_INVOICE_DISTRIBUTIONS_ALL.UNIT_PRICE * AP_INVOICE_DISTRIBUTIONS_ALL.QUANTITY_INVOICED
        ) * (
         NVL( case
            when AP_INVOICES_ALL.INVOICE_CURRENCY_CODE = GL_SETS_OF_BOOKS.CURRENCY_CODE THEN 1
            ELSE AP_INVOICES_ALL.EXCHANGE_RATE
          END,1)
        )
        ELSE (
          NVL(case
            when AP_INVOICES_ALL.INVOICE_CURRENCY_CODE = GL_SETS_OF_BOOKS.CURRENCY_CODE THEN 1
            ELSE AP_INVOICES_ALL.EXCHANGE_RATE
          END,1) * (
            AP_INVOICE_DISTRIBUTIONS_ALL.UNIT_PRICE * AP_INVOICE_DISTRIBUTIONS_ALL.QUANTITY_INVOICED
          ) * -1
        )
      END
    )
    ELSE AP_INVOICE_DISTRIBUTIONS_ALL.BASE_AMOUNT   end ,  
    (case
    when (
      AP_INVOICE_DISTRIBUTIONS_ALL.UNIT_PRICE * AP_INVOICE_DISTRIBUTIONS_ALL.QUANTITY_INVOICED
    ) is null then  AP_INVOICE_DISTRIBUTIONS_ALL.AMOUNT
    else (
      AP_INVOICE_DISTRIBUTIONS_ALL.UNIT_PRICE * AP_INVOICE_DISTRIBUTIONS_ALL.QUANTITY_INVOICED
    )
  end) *  NVL(case
            when AP_INVOICES_ALL.INVOICE_CURRENCY_CODE = GL_SETS_OF_BOOKS.CURRENCY_CODE THEN 1
            ELSE AP_INVOICES_ALL.EXCHANGE_RATE
          END ,1) ) *-1 as localAmount,
  PO_LINES_ALL.UNIT_MEAS_LOOKUP_CODE as orderUomCode,
  REPLACE(STRING(INT(AP_INVOICES_ALL.ORG_ID)), ",", "") as owningBusinessUnitId,
  AP_INVOICES_ALL.PAYMENT_METHOD_LOOKUP_CODE as paymentMethodId,
  terms.NAME as paymentTermId,
  AP_INVOICE_DISTRIBUTIONS_ALL.ACCOUNTING_DATE as postDate,
  REPLACE(
    STRING(INT(GL_ACCOUNT.profitCenterNumber)),
    ",",
    ""
  ) as profitCenterId,
  AP_INVOICE_DISTRIBUTIONS_ALL.DISTRIBUTION_LINE_NUMBER as purchaseInvoiceLine,
  AP_INVOICES_ALL.INVOICE_NUM as purchaseInvoiceNumber,
  PO_LINES_ALL.LINE_NUM as purchaseOrderLine,
  PO_HEADERS_ALL.SEGMENT1 as purchaseOrderNumber,
  AP_INVOICE_DISTRIBUTIONS_ALL.QUANTITY_INVOICED as quantityInvoiced,
  null as referenceDocumentItem,
  REPLACE(
    STRING(INT(AP_INVOICES_ALL.INVOICE_ID)),
    ",",
    ""
  ) as referenceDocumentNumber,
  null as remainingAmount,
  AP_INVOICE_DISTRIBUTIONS_ALL.reversal_flag as reversalFlag,
  REPLACE(STRING(INT(AP_INVOICES_ALL.VENDOR_ID)), ",", "") as supplierId,
  REPLACE(
    STRING(INT(AP_INVOICES_ALL.VENDOR_SITE_ID)),
    ",",
    ""
  ) as supplierSiteId,
  null as transactionDate,
  AP_INVOICE_DISTRIBUTIONS_ALL.AMOUNT as transactionAmount,
  'INVDIST-' || INT(AP_INVOICE_DISTRIBUTIONS_ALL.INVOICE_ID) || '~' || INT(AP_INVOICE_DISTRIBUTIONS_ALL.INVOICE_LINE_NUMBER) || '-' || INT(
    AP_INVOICE_DISTRIBUTIONS_ALL.DISTRIBUTION_LINE_NUMBER  ) transactionDetailId,
  AP_INVOICE_DISTRIBUTIONS_ALL.QUANTITY_INVOICED as transactionQuantity,
  AP_INVOICES_ALL.Total_tax_amount totalTaxAmount
from
  AP_INVOICE_DISTRIBUTIONS_ALL
  join ebs.AP_INVOICES_ALL on AP_INVOICES_ALL.INVOICE_ID = AP_INVOICE_DISTRIBUTIONS_ALL.INVOICE_ID
  left join ebs.PO_DISTRIBUTIONS_ALL on AP_INVOICE_DISTRIBUTIONS_ALL.PO_DISTRIBUTION_ID = PO_DISTRIBUTIONS_ALL.PO_DISTRIBUTION_ID
  left join ebs.PO_HEADERS_ALL on PO_DISTRIBUTIONS_ALL.PO_HEADER_ID = PO_HEADERS_ALL.PO_HEADER_ID
  left join ebs.PO_LINES_ALL on PO_DISTRIBUTIONS_ALL.PO_LINE_ID = PO_LINES_ALL.PO_LINE_ID
  left join ebs.ap_terms_tl terms on terms.term_id = AP_INVOICES_ALL.TERMS_ID
  left join GL_ACCOUNT on GL_ACCOUNT.glCombinationId =   REPLACE(
    STRING(
      INT(
        AP_INVOICE_DISTRIBUTIONS_ALL.DIST_CODE_COMBINATION_ID
      )
    ),
    ",",
    ""
  ) 
  left join ebs.GL_SETS_OF_BOOKS on AP_INVOICES_ALL.SET_OF_BOOKS_ID = GL_SETS_OF_BOOKS.SET_OF_BOOKS_ID
  LEFT OUTER JOIN S_CORE.EXCHANGE_RATE_AGG ON 
      'AVERAGE' || '-' || case when length(trim(AP_INVOICES_ALL.INVOICE_CURRENCY_CODE)) = 0 then 'USD' else  trim(AP_INVOICES_ALL.INVOICE_CURRENCY_CODE) end || '-USD-' || date_format(AP_INVOICE_DISTRIBUTIONS_ALL.ACCOUNTING_DATE, 'yyyy-MM') || '-01-QV' = EXCHANGE_RATE_AGG.fxID
  
where
  AP_INVOICES_ALL.ACCTS_PAY_CODE_COMBINATION_ID IS NOT NULL
  and terms.language = 'US'
 -- and AP_INVOICES_ALL.INVOICE_NUM = '000011976'
  """)
main_expense.createOrReplaceTempView("main_expense")

# COMMAND ----------

main_liability = spark.sql("""
--LIABILITY
SELECT
  REPLACE(
    STRING(INT(AP_INVOICE_DISTRIBUTIONS_ALL.CREATED_BY)),
    ",",
    ""
  ) as createdBy,
  AP_INVOICE_DISTRIBUTIONS_ALL.CREATION_DATE as createdOn,
  REPLACE(
    STRING(
      INT(AP_INVOICE_DISTRIBUTIONS_ALL.LAST_UPDATED_BY)
    ),
    ",",
    ""
  ) AS modifiedBy,
  AP_INVOICE_DISTRIBUTIONS_ALL.LAST_UPDATE_DATE AS modifiedOn,
  CURRENT_TIMESTAMP() as insertedOn,
  CURRENT_TIMESTAMP() as updatedOn,
  'AP_INV_DIST-LIABILITY' || '-' || INT(INVOICE_DISTRIBUTION_ID) accountDocId,
  AP_INVOICE_DISTRIBUTIONS_ALL.BASE_AMOUNT as baseAmount,
  'COMPANY-' || REPLACE(
    STRING(INT(AP_INVOICES_ALL.LEGAL_ENTITY_ID)),
    ",",
    ""
  ) as companyId,
  GL_ACCOUNT.costCenterNumber as costCenterId,
  AP_INVOICES_ALL.INVOICE_CURRENCY_CODE as currency,
  AP_INVOICES_ALL.INVOICE_DATE as dateInvoiced,
  CASE
    WHEN -1 *(AP_INVOICE_DISTRIBUTIONS_ALL.AMOUNT) < 0 then 'CREDIT'
    else 'DEBIT'
  end as debitCreditFlag,
  NULL as description,
  AP_INVOICE_DISTRIBUTIONS_ALL.DISTRIBUTION_LINE_NUMBER as distributionLineNumber,
  'LIABILITY' as distributionType,
  case
    when (
      AP_INVOICE_DISTRIBUTIONS_ALL.UNIT_PRICE * AP_INVOICE_DISTRIBUTIONS_ALL.QUANTITY_INVOICED
    ) is null then -1 * AP_INVOICE_DISTRIBUTIONS_ALL.AMOUNT
    else (
      AP_INVOICE_DISTRIBUTIONS_ALL.UNIT_PRICE * AP_INVOICE_DISTRIBUTIONS_ALL.QUANTITY_INVOICED*-1
    )
  end as documentAmount,
  AP_INVOICES_ALL.SOURCE as documentSource,
  'ACCT_DOC_STATUS-' || 'OPEN-UNPOSTED' as documentStatus,
  'ACCT_DOC' || '-' || 'PAYABLES' || '-' || 'DISTRIBUTION' || '-' || AP_INVOICES_ALL.INVOICE_TYPE_LOOKUP_CODE as documentTypeId,
  null as dueDate,
  NVL(case when AP_INVOICES_ALL.INVOICE_CURRENCY_CODE = GL_SETS_OF_BOOKS.CURRENCY_CODE THEN 1 ELSE AP_INVOICES_ALL.EXCHANGE_RATE END , 1) as exchangeRate,
  EXCHANGE_RATE_AGG.exchangeRate exchangeRateUSD,
  REPLACE(
    STRING(
      INT(
        AP_INVOICES_ALL.ACCTS_PAY_CODE_COMBINATION_ID
      )
    ),
    ",",
    ""
  ) as glAccountId,
  INT(AP_INVOICES_ALL.SET_OF_BOOKS_ID) || '-' || INT(AP_INVOICES_ALL.ACCTS_PAY_CODE_COMBINATION_ID) || '-' || INT(AP_INVOICES_ALL.ORG_ID) as glBalanceId,
  null as glJournalId,
  AP_INVOICES_ALL.Invoice_Amount invoiceHeaderAmount,
  REPLACE(
    STRING(INT(AP_INVOICES_ALL.INVOICE_ID)),
    ",",
    ""
  ) as invoiceId,
  AP_INVOICE_DISTRIBUTIONS_ALL.INVOICE_LINE_NUMBER as invoiceLineNumber,
  AP_INVOICES_ALL.INVOICE_RECEIVED_DATE as invoiceReceivedDate,
  PO_LINES_ALL.ITEM_ID as itemId,
  REPLACE(
    STRING(INT(AP_INVOICES_ALL.SET_OF_BOOKS_ID)),
    ",",
    ""
  ) as ledgerId,
  
  nvl(
   CASE
    WHEN AP_INVOICE_DISTRIBUTIONS_ALL.BASE_AMOUNT IS NULL THEN (
      CASE
        WHEN (
          NVL(case
            when AP_INVOICES_ALL.INVOICE_CURRENCY_CODE = GL_SETS_OF_BOOKS.CURRENCY_CODE THEN 1
            ELSE AP_INVOICES_ALL.EXCHANGE_RATE
          END,1)
        ) IS NULL
        AND (
          AP_INVOICE_DISTRIBUTIONS_ALL.UNIT_PRICE * AP_INVOICE_DISTRIBUTIONS_ALL.QUANTITY_INVOICED
        ) IS NULL THEN (
          AP_INVOICE_DISTRIBUTIONS_ALL.UNIT_PRICE * AP_INVOICE_DISTRIBUTIONS_ALL.QUANTITY_INVOICED
        ) * (
          NVL(case
            when AP_INVOICES_ALL.INVOICE_CURRENCY_CODE = GL_SETS_OF_BOOKS.CURRENCY_CODE THEN 1
            ELSE AP_INVOICES_ALL.EXCHANGE_RATE
          END,1)
        )
        ELSE (
          NVL(case
            when AP_INVOICES_ALL.INVOICE_CURRENCY_CODE = GL_SETS_OF_BOOKS.CURRENCY_CODE THEN 1
            ELSE AP_INVOICES_ALL.EXCHANGE_RATE
          END ,1) * (
            AP_INVOICE_DISTRIBUTIONS_ALL.UNIT_PRICE * AP_INVOICE_DISTRIBUTIONS_ALL.QUANTITY_INVOICED
          ) * -1
        )
      END
    )
    ELSE AP_INVOICE_DISTRIBUTIONS_ALL.BASE_AMOUNT
  end,   (case
    when (
      AP_INVOICE_DISTRIBUTIONS_ALL.UNIT_PRICE * AP_INVOICE_DISTRIBUTIONS_ALL.QUANTITY_INVOICED
    ) is null then  AP_INVOICE_DISTRIBUTIONS_ALL.AMOUNT
    else (
      AP_INVOICE_DISTRIBUTIONS_ALL.UNIT_PRICE * AP_INVOICE_DISTRIBUTIONS_ALL.QUANTITY_INVOICED
    )
  end) *  NVL(case
            when AP_INVOICES_ALL.INVOICE_CURRENCY_CODE = GL_SETS_OF_BOOKS.CURRENCY_CODE THEN 1
            ELSE AP_INVOICES_ALL.EXCHANGE_RATE
          END ,1) ) *-1 as localAmount,
  PO_LINES_ALL.UNIT_MEAS_LOOKUP_CODE as orderUomCode,
  REPLACE(STRING(INT(AP_INVOICES_ALL.ORG_ID)), ",", "") as owningBusinessUnitId,
  AP_INVOICES_ALL.PAYMENT_METHOD_LOOKUP_CODE as paymentMethodId,
  terms.NAME as paymentTermId,
  AP_INVOICE_DISTRIBUTIONS_ALL.ACCOUNTING_DATE as postDate,
  REPLACE(
    STRING(INT(GL_ACCOUNT.profitCenterNumber)),
    ",",
    ""
  ) as profitCenterId,
  AP_INVOICE_DISTRIBUTIONS_ALL.DISTRIBUTION_LINE_NUMBER as purchaseInvoiceLine,
  AP_INVOICES_ALL.INVOICE_NUM as purchaseInvoiceNumber,
  PO_LINES_ALL.LINE_NUM as purchaseOrderLine,
  PO_HEADERS_ALL.SEGMENT1 as purchaseOrderNumber,
  AP_INVOICE_DISTRIBUTIONS_ALL.QUANTITY_INVOICED as quantityInvoiced,
  null as referenceDocumentItem,
  REPLACE(
    STRING(INT(AP_INVOICES_ALL.INVOICE_ID)),
    ",",
    ""
  ) as referenceDocumentNumber,
  null as remainingAmount,
  AP_INVOICE_DISTRIBUTIONS_ALL.reversal_flag as reversalFlag,
  REPLACE(STRING(INT(AP_INVOICES_ALL.VENDOR_ID)), ",", "") as supplierId,
  REPLACE(
    STRING(INT(AP_INVOICES_ALL.VENDOR_SITE_ID)),
    ",",
    ""
  ) as supplierSiteId,
  null as transactionDate,
  AP_INVOICE_DISTRIBUTIONS_ALL.AMOUNT as transactionAmount,
  'DIST-' || INT(AP_INVOICE_DISTRIBUTIONS_ALL.INVOICE_ID) || '~' || INT(AP_INVOICE_DISTRIBUTIONS_ALL.INVOICE_LINE_NUMBER) || '-' || INT(
    AP_INVOICE_DISTRIBUTIONS_ALL.DISTRIBUTION_LINE_NUMBER  ) transactionDetailId,
  AP_INVOICE_DISTRIBUTIONS_ALL.QUANTITY_INVOICED as transactionQuantity,
  AP_INVOICES_ALL.Total_tax_amount totalTaxAmount
from
  AP_INVOICE_DISTRIBUTIONS_ALL
  join ebs.AP_INVOICES_ALL on AP_INVOICES_ALL.INVOICE_ID = AP_INVOICE_DISTRIBUTIONS_ALL.INVOICE_ID
  left join ebs.PO_DISTRIBUTIONS_ALL on AP_INVOICE_DISTRIBUTIONS_ALL.PO_DISTRIBUTION_ID = PO_DISTRIBUTIONS_ALL.PO_DISTRIBUTION_ID
  left join ebs.PO_HEADERS_ALL on PO_DISTRIBUTIONS_ALL.PO_HEADER_ID = PO_HEADERS_ALL.PO_HEADER_ID
  left join ebs.PO_LINES_ALL on PO_DISTRIBUTIONS_ALL.PO_LINE_ID = PO_LINES_ALL.PO_LINE_ID
  left join ebs.ap_terms_tl terms on terms.term_id = AP_INVOICES_ALL.TERMS_ID
  left join GL_ACCOUNT on GL_ACCOUNT.glCombinationId = REPLACE(
    STRING(
      INT(
         AP_INVOICES_ALL.ACCTS_PAY_CODE_COMBINATION_ID
      )
    ),
    ",",
    ""
  )
left join ebs.GL_SETS_OF_BOOKS on AP_INVOICES_ALL.SET_OF_BOOKS_ID = GL_SETS_OF_BOOKS.SET_OF_BOOKS_ID
  LEFT OUTER JOIN S_CORE.EXCHANGE_RATE_AGG ON 
      'AVERAGE' || '-' || case when length(trim(AP_INVOICES_ALL.INVOICE_CURRENCY_CODE)) = 0 then 'USD' else  trim(AP_INVOICES_ALL.INVOICE_CURRENCY_CODE) end || '-USD-' || date_format(AP_INVOICE_DISTRIBUTIONS_ALL.ACCOUNTING_DATE, 'yyyy-MM') || '-01-QV' = EXCHANGE_RATE_AGG.fxID
where
  AP_INVOICES_ALL.ACCTS_PAY_CODE_COMBINATION_ID IS NOT NULL
  and terms.language = 'US' 
  --and AP_INVOICES_ALL.INVOICE_NUM = '000011976'
   """)
main_liability.createOrReplaceTempView("main_liability")

# COMMAND ----------

main = spark.sql("""
 SELECT * FROM main_liability
 union
  SELECT * FROM main_expense
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
  .transform(tg_finance_ap_transactions())
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

keys_expense = (
  spark.sql("""
    SELECT   
    'INVDIST-' || INT(AP_INVOICE_DISTRIBUTIONS_ALL.INVOICE_ID) || '~' || INT(AP_INVOICE_DISTRIBUTIONS_ALL.INVOICE_LINE_NUMBER) || '-' || INT(
    AP_INVOICE_DISTRIBUTIONS_ALL.DISTRIBUTION_LINE_NUMBER  ) transactionDetailId
    FROM
  ebs.AP_INVOICE_DISTRIBUTIONS_ALL
  join ebs.AP_INVOICES_ALL on AP_INVOICES_ALL.INVOICE_ID = AP_INVOICE_DISTRIBUTIONS_ALL.INVOICE_ID
   left join ebs.PO_DISTRIBUTIONS_ALL on AP_INVOICE_DISTRIBUTIONS_ALL.PO_DISTRIBUTION_ID = PO_DISTRIBUTIONS_ALL.PO_DISTRIBUTION_ID
  where
  AP_INVOICES_ALL.ACCTS_PAY_CODE_COMBINATION_ID IS NOT NULL
  """)
)


keys_liability = (
  spark.sql("""
   SELECT   
      'DIST-' || INT(AP_INVOICE_DISTRIBUTIONS_ALL.INVOICE_ID) || '~' || INT(AP_INVOICE_DISTRIBUTIONS_ALL.INVOICE_LINE_NUMBER) || '-' || INT(
    AP_INVOICE_DISTRIBUTIONS_ALL.DISTRIBUTION_LINE_NUMBER  ) transactionDetailId
     FROM
  ebs.AP_INVOICE_DISTRIBUTIONS_ALL
  join ebs.AP_INVOICES_ALL on AP_INVOICES_ALL.INVOICE_ID = AP_INVOICE_DISTRIBUTIONS_ALL.INVOICE_ID
   left join ebs.PO_DISTRIBUTIONS_ALL on AP_INVOICE_DISTRIBUTIONS_ALL.PO_DISTRIBUTION_ID = PO_DISTRIBUTIONS_ALL.PO_DISTRIBUTION_ID
  where
  AP_INVOICES_ALL.ACCTS_PAY_CODE_COMBINATION_ID IS NOT NULL
  """)
)

full_keys_f = (
  keys_expense
  .union(keys_liability)
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
  cutoff_value = get_incr_col_max_value(ap_invoice_distributions_all,'_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 'ebs.ap_invoice_distributions_all')
  update_run_datetime(run_datetime, table_name, 'ebs.ap_invoice_distributions_all')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_finance
