# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_trademanagement

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_trademanagement.pricelistheader

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'ebs.qp_list_headers_b', prune_days)
  main_inc = load_incr_dataset('ebs.qp_list_headers_b', 'LAST_UPDATE_DATE', cutoff_value)
else:
  main_inc = load_full_dataset('ebs.qp_list_headers_b')

# COMMAND ----------

# SAMPLING
if sampling:
  main_inc = main_inc.limit(10)

# COMMAND ----------

# VIEWS
main_inc.createOrReplaceTempView('qp_list_headers_b')

# COMMAND ----------

main = spark.sql("""
SELECT
REPLACE(STRING(INT (QP_LIST_HEADERS_B.CREATED_BY)), ",", "") AS createdBy,
QP_LIST_HEADERS_B.CREATION_DATE AS createdOn,
REPLACE(STRING(INT (QP_LIST_HEADERS_B.LAST_UPDATED_BY)), ",", "") AS modifiedBy,
QP_LIST_HEADERS_B.LAST_UPDATE_DATE AS modifiedOn,
CURRENT_DATE() AS insertedOn,
CURRENT_DATE() AS updatedOn,
QP_LIST_HEADERS_B.CURRENCY_CODE AS currencyCode,
QP_LIST_HEADERS_B.END_DATE_ACTIVE AS endDate,
QP_LIST_HEADERS_B.FREIGHT_TERMS_CODE AS freightTermsCode,
QP_LIST_HEADERS_B.LIST_TYPE_CODE AS listTypeCode,
QP_LIST_HEADERS_B.COMMENTS AS priceListComment,
QP_LIST_HEADERS_TL.DESCRIPTION AS priceListDescription,
REPLACE(STRING(INT (QP_LIST_HEADERS_B.LIST_HEADER_ID)), ",", "") AS priceListHeaderId,
QP_LIST_HEADERS_TL.NAME AS priceListName,
QP_LIST_HEADERS_B.PROGRAM_APPLICATION_ID AS programApplicationId,
QP_LIST_HEADERS_B.PROGRAM_ID AS programId,
QP_LIST_HEADERS_B.PROGRAM_UPDATE_DATE AS programUpdateDate,
QP_LIST_HEADERS_B.ROUNDING_FACTOR AS roundingFactor,
QP_LIST_HEADERS_B.SHIP_METHOD_CODE AS shipMethodCode,
QP_LIST_HEADERS_B.START_DATE_ACTIVE AS startDate 
FROM 
QP_LIST_HEADERS_B
JOIN EBS.QP_LIST_HEADERS_TL ON QP_LIST_HEADERS_TL.LIST_HEADER_ID = QP_LIST_HEADERS_B.LIST_HEADER_ID
WHERE 
QP_LIST_HEADERS_TL.LANGUAGE = 'US'

""")

main.createOrReplaceTempView('main')

# COMMAND ----------

columns = list(schema.keys())
key_columns = ['priceListHeaderId']

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .transform(tg_default(source_name))
  .transform(tg_trade_management_price_list_header())
  .transform(apply_schema(schema))
  .transform(attach_unknown_record)
  .select(columns)
  .transform(sort_columns)    
)

# COMMAND ----------

# PERSIST DATA
options = {'target_storage': target_storage, 'target_container': target_container}
merge_to_delta(main_f, table_name, target_folder, overwrite, options = options)

# COMMAND ----------

# HANDLE DELETE
full_keys_f = (
  spark.table('EBS.QP_LIST_HEADERS_B')
  .selectExpr('REPLACE(STRING(INT (LIST_HEADER_ID)), ",", "") AS priceListHeaderId')
  .transform(attach_source_column(source = source_name))
  .transform(attach_primary_key('priceListHeaderId,_SOURCE', 'edm.price_list_header'))
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
  
  cutoff_value = get_incr_col_max_value(main_inc, 'LAST_UPDATE_DATE')
  update_cutoff_value(cutoff_value, table_name, 'ebs.qp_list_headers_b')
  update_run_datetime(run_datetime, table_name, 'ebs.qp_list_headers_b')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_trademanagement
