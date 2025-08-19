# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.exchangerate

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'ebs.gl_daily_rates', prune_days)
  main_inc = load_incr_dataset('ebs.gl_daily_rates', 'LAST_UPDATE_DATE', cutoff_value)
else:
  main_inc = load_full_dataset('ebs.gl_daily_rates')

# COMMAND ----------

# SAMPLING
if sampling:
  main_inc = main_inc.limit(10)

# COMMAND ----------

# VIEWS
main_inc.createOrReplaceTempView('gl_daily_rates')

# COMMAND ----------

main = spark.sql("""
SELECT
     REPLACE(STRING(INT (GL_DAILY_RATES.CREATED_BY)),
    ",",
    ""
     ) AS createdBy,
    GL_DAILY_RATES.CREATION_DATE AS createdOn,
     REPLACE(STRING(INT (GL_DAILY_RATES.LAST_UPDATED_BY )),
    ",",
    ""
    ) AS modifiedBy,
    GL_DAILY_RATES.LAST_UPDATE_DATE AS modifiedOn,
    CURRENT_TIMESTAMP() AS insertedOn,
    CURRENT_TIMESTAMP() AS updatedOn, 
    GL_DAILY_RATES.CONVERSION_TYPE AS rateType,
    GL_DAILY_RATES.FROM_CURRENCY AS fromCurrency,
    GL_DAILY_RATES.TO_CURRENCY AS toCurrency,
    GL_DAILY_RATES.CONVERSION_DATE AS conversionDate,
    GL_DAILY_RATES.CONVERSION_DATE AS startDate,
   DATE_ADD(GL_DAILY_RATES.CONVERSION_DATE,1) AS endDate,
    GL_DAILY_RATES.CONVERSION_RATE AS exchangeRate,
    '001' AS client,
    CONCAT(GL_DAILY_RATES.CONVERSION_TYPE,'-',GL_DAILY_RATES.FROM_CURRENCY,'-',GL_DAILY_RATES.TO_CURRENCY,'-',CAST(GL_DAILY_RATES.CONVERSION_DATE AS DATE),'-','EBS') as fxID
  FROM GL_DAILY_RATES
  
  
 """)

main.createOrReplaceTempView('main')
main.cache()
display(main)

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

main = remove_duplicate_rows(df = main, 
                             key_columns = ['fxID'], 
                             tableName = table_name, 
                             sourceName = source_name, 
                             notebookName = NOTEBOOK_NAME, 
                             notebookPath = NOTEBOOK_PATH)

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .transform(tg_default(source_name))
  .transform(tg_core_exchange_rate())
  .transform(apply_schema(schema))
  .transform(attach_unknown_record)
  .select(columns)
  .transform(sort_columns)
)

main_f.cache()
main_f.display()

# COMMAND ----------

# PERSIST DATA
options = {'target_storage': target_storage, 'target_container': target_container}
merge_to_delta(main_f, table_name, target_folder, overwrite, options = options)

# COMMAND ----------

# HANDLE DELETE
full_keys_f = (
  spark.table('ebs.gl_daily_rates')
  .selectExpr("CONCAT(GL_DAILY_RATES.CONVERSION_TYPE,'-',GL_DAILY_RATES.FROM_CURRENCY,'-',GL_DAILY_RATES.TO_CURRENCY,'-',CAST(GL_DAILY_RATES.CONVERSION_DATE AS DATE),'-','EBS') as fxID")
  .transform(attach_source_column(source = source_name))
  .transform(attach_surrogate_key(columns = 'fxID,_SOURCE'))
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
  update_cutoff_value(cutoff_value, table_name, 'ebs.gl_daily_rates')
  update_run_datetime(run_datetime, table_name, 'ebs.gl_daily_rates')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core
