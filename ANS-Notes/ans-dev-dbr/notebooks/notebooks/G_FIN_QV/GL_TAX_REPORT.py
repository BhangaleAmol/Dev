# Databricks notebook source
# MAGIC %sql 
# MAGIC CREATE OR REPLACE VIEW g_fin_qv.gl_tax_report AS
# MAGIC SELECT
# MAGIC DATE.fiscalYearId FISCAL_YEAR,
# MAGIC   UPPER(DATE.periodName) FISCAL_PERIOD,
# MAGIC   DIVISION.segmentValueCode DIVISION_CODE,
# MAGIC   DIVISION.segmentValueDescription DIVISION_NAME,
# MAGIC   ENTITY.segmentValueCode AS LEGAL_ENTITY_CODE,
# MAGIC   ENTITY.segmentValueDescription AS LEGAL_ENTITY_NAME,
# MAGIC   REGION.segmentValueCode REGION_CODE,
# MAGIC   REGION.segmentValueDescription REGION_NAME,
# MAGIC   FUT.segmentValueCode AS GL_FUTURE_CODE,
# MAGIC   FUT.segmentValueDescription AS GL_FUTURE_NAME,
# MAGIC   ACC.glAccountNumber AS GL_ACCOUNT_NUM,
# MAGIC   ACC.glAccountCategoryCode GL_ACCOUNT_CATEGORY,
# MAGIC   GL.glJournalId AS GL_JOURNAL_ID,
# MAGIC   GL.glJournalCategory AS GL_JOURNAL_CATEGORY,
# MAGIC   GL.lineItemText AS ACCOUNTING_DOCUMENT_NAME,
# MAGIC   GL.referenceDocumentNumber ACCOUNTING_DOCUMENT_SOURCE,
# MAGIC   GL.accountingDocumentNumber ACCOUNTING_DOCUMENT_NUMBER,
# MAGIC   CC.segmentValueCode COST_CENTER_CODE,
# MAGIC   CC.segmentValueDescription COST_CENTER_NAME,
# MAGIC   GL.amountDocumentCurrency TRANSACTION_AMOUNT,
# MAGIC   GL.amountDocumentCurrency/NVL(GL.exchangeRateUsd,GL.exchangeRateAverageUsd) TRANSACTION_AMOUNT_USD
# MAGIC   
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
# MAGIC   AND LEDGER.NAME NOT IN ('MX Local Ledger')
# MAGIC   --AND ENTITY.segmentValueCode NOT IN ('2190')
# MAGIC   AND (REGION.segmentValueCode='00000' OR REGION.segmentValueCode = '10100')
# MAGIC   AND  DIVISION.segmentValueCode IN ('300','305','31X','3XX','400','405','410','415','41X','320')
# MAGIC   AND CC.segmentValueCode in ( '6400','6450')
# MAGIC   --and DATE.fiscalYearId <= date_format(current_date,'yyyy')-3
# MAGIC   --AND UPPER(DATE.periodName) = 'APR-22'
# MAGIC   --AND DATE.fiscalYearId >= date_format(add_months(current_date -30, 6), 'yyyy')
