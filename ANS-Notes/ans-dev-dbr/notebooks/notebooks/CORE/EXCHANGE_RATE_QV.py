# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.exchangerate

# COMMAND ----------

# LOAD DATASETS
fx_rates = spark.table('amazusftp1.fx_rates')
if sampling: fx_rates = fx_rates.limit(10)
fx_rates.createOrReplaceTempView('fx_rates')
display(fx_rates)

fx_rates_hist = spark.table('amazusftp1.fx_rates_hist')
if sampling: fx_rates_hist = fx_rates_hist.limit(10)
fx_rates_hist.createOrReplaceTempView('fx_rates_hist')
display(fx_rates_hist)

# COMMAND ----------

main = spark.sql("""
SELECT
    cast(NULL as string) AS createdBy,
    cast(NULL AS timestamp) AS createdOn,
    cast(NULL AS STRING) AS modifiedBy,
    cast(NULL AS timestamp) AS modifiedOn,
    CURRENT_TIMESTAMP() AS insertedOn,
    CURRENT_TIMESTAMP() AS updatedOn, 
    'AVERAGE' AS rateType,
    rateCode AS fromCurrency,
   'USD' AS toCurrency,
   to_date(concat('20',ratemonth,'01'),'yyyyMMdd') AS conversionDate,
   to_date(concat('20',ratemonth,'01'),'yyyyMMdd') AS startDate,
   last_day(to_date(concat('20',ratemonth,'01'),'yyyyMMdd')) AS endDate,
    exchangeRate,
    '001' AS client,
    CONCAT('AVERAGE','-',rateCode,'-','USD','-',to_date(concat('20',ratemonth,'01'),'yyyyMMdd'),'-','QV') as fxID
  FROM amazusftp1.fx_rates
  
  union
  
SELECT
    cast(NULL as string) AS createdBy,
    cast(NULL AS timestamp) AS createdOn,
    cast(NULL AS STRING) AS modifiedBy,
    cast(NULL AS timestamp) AS modifiedOn,
    CURRENT_TIMESTAMP() AS insertedOn,
    CURRENT_TIMESTAMP() AS updatedOn, 
    'AVERAGE' AS rateType,
    rateCode AS fromCurrency,
   'USD' AS toCurrency,
   to_date(concat('20',ratemonth,'01'),'yyyyMMdd') AS conversionDate,
   to_date(concat('20',ratemonth,'01'),'yyyyMMdd') AS startDate,
   last_day(to_date(concat('20',ratemonth,'01'),'yyyyMMdd')) AS endDate,
    exchangeRate,
    '001' AS client,
    CONCAT('AVERAGE','-',rateCode,'-','USD','-',to_date(concat('20',ratemonth,'01'),'yyyyMMdd'),'-','QV') as fxID
  FROM amazusftp1.fx_rates_hist
  
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

# UPDATE FK
update_foreign_key(table_name, 'createdBy,_SOURCE', 'createdBy_ID', 'edm.user')
update_foreign_key(table_name, 'modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core
