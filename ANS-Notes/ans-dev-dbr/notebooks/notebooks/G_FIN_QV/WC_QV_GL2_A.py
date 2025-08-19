# Databricks notebook source
# MAGIC %sql 
# MAGIC CREATE OR REPLACE VIEW g_fin_qv.wc_qv_gl2_a AS
# MAGIC SELECT
# MAGIC   UPPER(DATE.periodName) FISCAL_PERIOD,
# MAGIC   ENTITY.segmentValueDescription AS LEGAL_ENTITY_NAME,
# MAGIC   ENTITY.segmentValueCode AS LEGAL_ENTITY_CODE,
# MAGIC   DIVISION.segmentValueDescription DIVISION_NAME,
# MAGIC   REGION.segmentValueCode REGION_CODE,
# MAGIC   REGION.segmentValueDescription REGION_NAME,
# MAGIC   CC.segmentValueCode COST_CENTER_CODE,
# MAGIC   CC.segmentValueDescription COST_CENTER_NAME,
# MAGIC   ACC.glAccountNumber AS GL_ACCOUNT_NUM,
# MAGIC   ACC_NAME.segmentValueDescription AS GL_ACCOUNT_DESC,
# MAGIC   FUT.segmentValueCode AS GL_FUTURE_CODE,
# MAGIC   FUT.segmentValueDescription AS GL_FUTURE_NAME,
# MAGIC   GL.lineItemText AS ACCOUNTING_DOCUMENT_NAME,
# MAGIC   GL.referenceDocumentNumber ACCOUNTING_DOCUMENT_SOURCE,
# MAGIC   GL.accountingDocumentNumber ACCOUNTING_DOCUMENT_NUMBER,
# MAGIC   SUM(GL.amountDocumentCurrency / nvl(exchangeRateUsd,1)) TRANSACTION_AMOUNT_USD,
# MAGIC   SUM(GL.amountDocumentCurrency) TRANSACTION_AMOUNT,
# MAGIC   GL.localCurrencyCode AS LEDGER_CURRENCY,
# MAGIC   DATE.fiscalYearId AS FISCAL_YEAR,
# MAGIC   cast(GL.postedOnDate as date) AS TRANSACTION_DATE,
# MAGIC   cast(DATE.monthEndDate as date) AS MONTH_END_DATE
# MAGIC FROM
# MAGIC   S_FINANCE.GL_TRANSACTIONS_EBS GL
# MAGIC   JOIN S_CORE.DATE ON DATE_FORMAT(GL.enterpriseEndDate, 'yyyyMMdd') = DATE.DAYID
# MAGIC   JOIN S_FINANCE.GL_SEGMENTS_EBS ENTITY ON GL.legalEntity_Id = ENTITY._ID
# MAGIC   JOIN S_FINANCE.GL_SEGMENTS_EBS DIVISION ON GL.division_Id = DIVISION._ID
# MAGIC   JOIN S_FINANCE.GL_SEGMENTS_EBS REGION ON GL.region_Id = REGION._ID
# MAGIC   JOIN S_FINANCE.GL_SEGMENTS_EBS CC ON GL.costCenter_Id = CC._ID
# MAGIC   JOIN S_FINANCE.GL_ACCOUNTS_EBS ACC ON GL.glaccountid = acc.glCombinationId
# MAGIC   JOIN S_FINANCE.GL_SEGMENTS_EBS FUT ON GL.future_Id = FUT._ID
# MAGIC   JOIN S_FINANCE.GL_SEGMENTS_EBS ACC_NAME ON ACC.segment2Attribute = ACC_NAME.segmentlovid
# MAGIC   and ACC.segment2Code = ACC_NAME.segmentvaluecode
# MAGIC   JOIN S_CORE.LEDGER_EBS LEDGER ON GL.LEDGER_ID = LEDGER._ID
# MAGIC WHERE
# MAGIC   1 = 1 -- AND GLJOURNALID = '1638443-2'
# MAGIC   AND LEDGER.NAME NOT IN ('MX Local Ledger','IN PP Local Ledger')
# MAGIC   AND ACC.glAccountNumber >= '600000'
# MAGIC   AND CC.segmentValueCode IN ('6450', '6400')
# MAGIC   AND DATE.fiscalYearId >= date_format(add_months(current_date -30, 6), 'yyyy')
# MAGIC   GROUP BY 
# MAGIC    UPPER(DATE.periodName),
# MAGIC   ENTITY.segmentValueDescription,
# MAGIC   ENTITY.segmentValueCode ,
# MAGIC   DIVISION.segmentValueDescription ,
# MAGIC   REGION.segmentValueCode ,
# MAGIC   REGION.segmentValueDescription ,
# MAGIC   CC.segmentValueCode ,
# MAGIC   CC.segmentValueDescription,
# MAGIC   ACC.glAccountNumber ,
# MAGIC   ACC_NAME.segmentValueDescription ,
# MAGIC   FUT.segmentValueCode ,
# MAGIC   FUT.segmentValueDescription ,
# MAGIC   GL.lineItemText ,
# MAGIC   GL.referenceDocumentNumber ,
# MAGIC   GL.accountingDocumentNumber ,
# MAGIC   GL.localCurrencyCode ,
# MAGIC   DATE.fiscalYearId ,
# MAGIC   cast(GL.postedOnDate as date) ,
# MAGIC   cast(DATE.monthEndDate as date)
