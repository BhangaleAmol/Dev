# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_trademanagement

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_trademanagement.pricelistqualifier

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'ebs.qp_qualifiers', prune_days)
  main_inc = load_incr_dataset('ebs.qp_qualifiers', 'LAST_UPDATE_DATE', cutoff_value)
else:
  main_inc = load_full_dataset('ebs.qp_qualifiers')

# COMMAND ----------

# SAMPLING
if sampling:
  main_inc = main_inc.limit(10)

# COMMAND ----------

# VIEWS
main_inc.createOrReplaceTempView('qp_qualifiers')

# COMMAND ----------

main = spark.sql("""
SELECT
REPLACE(STRING(INT (QP_QUALIFIERS.CREATED_BY)), ",", "") AS createdBy,
QP_QUALIFIERS.CREATION_DATE AS createdOn,
REPLACE(STRING(INT (QP_QUALIFIERS.LAST_UPDATED_BY)), ",", "") AS modifiedBy,
QP_QUALIFIERS.LAST_UPDATE_DATE AS modifiedOn,
CURRENT_TIMESTAMP AS insertedOn,
CURRENT_TIMESTAMP AS updatedOn,
QP_QUALIFIERS.COMPARISON_OPERATOR_CODE AS comparisonOperatorCode,
REPLACE(STRING(INT (QP_QUALIFIERS.LIST_HEADER_ID)), ",", "") AS priceListHeaderId,
QP_QUALIFIERS.QUALIFIER_ATTRIBUTE AS qualifierAttribute,
QP_QUALIFIERS.QUALIFIER_ATTR_VALUE AS qualifierAttributeValue,
QP_QUALIFIERS.QUALIFIER_CONTEXT AS qualifierContext,
QP_QUALIFIERS.QUALIFIER_GROUPING_NO AS qualifierGroupingNumber,
REPLACE(STRING(INT (QP_QUALIFIERS.QUALIFIER_ID)), ",", "") AS qualifierId,
QP_QUALIFIERS.QUALIFIER_PRECEDENCE AS qualifierPrecedence
FROM 
QP_QUALIFIERS

""")

main.createOrReplaceTempView('main')

# COMMAND ----------

columns = list(schema.keys())
key_columns = ['qualifierId']

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .transform(tg_default(source_name))
  .transform(tg_trade_management_price_list_qualifier())
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
  spark.table('EBS.QP_QUALIFIERS')
  .selectExpr('REPLACE(STRING(INT (QP_QUALIFIERS.QUALIFIER_ID)), ",", "") AS qualifierId')
  .transform(attach_source_column(source = source_name))
  .transform(attach_primary_key('qualifierId,_SOURCE', 'edm.price_list_qualifier'))
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
  update_cutoff_value(cutoff_value, table_name, 'ebs.qp_qualifiers')
  update_run_datetime(run_datetime, table_name, 'ebs.qp_qualifiers')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_trademanagement
