# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.exchangerate

# COMMAND ----------

# LOAD DATASETS
fx_rates = spark.table('edmftp.fx_budget_rates')
if sampling: fx_rates = fx_rates.limit(10)
fx_rates.createOrReplaceTempView('fx_budget_rates')
display(fx_rates)

# COMMAND ----------

main = spark.sql("""
SELECT
    cast(NULL as string) AS createdBy,
    _MODIFIED AS createdOn,
    cast(NULL AS STRING) AS modifiedBy,
    _MODIFIED AS modifiedOn,
    CURRENT_TIMESTAMP() AS insertedOn,
    CURRENT_TIMESTAMP() AS updatedOn, 
    'BUDGET' AS rateType,
    Currency AS fromCurrency,
    'USD' AS toCurrency,
    to_date(concat('20' , INT(FiscalYear) -1 , '0701'),'yyyyMMdd') AS conversionDate,
    to_date(concat('20' , INT(FiscalYear) -1 , '0701'),'yyyyMMdd') AS startDate,
    last_day(to_date(concat('20' , INT(FiscalYear) , '0630'),'yyyyMMdd')) AS endDate,
    BudgetRate AS exchangeRate,
    '001' AS client,
    CONCAT('BUDGET','-',Currency,'-','USD','-',to_date(concat('20',INT(FiscalYear) -1 , '0701'),'yyyyMMdd'),'-','BUD') as fxID
  FROM fx_budget_rates  
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
  spark.table('edmftp.fx_budget_rates')
  .selectExpr("CONCAT('BUDGET','-',Currency,'-','USD','-',to_date(concat('20',INT(FiscalYear) -1 , '0701'),'yyyyMMdd'),'-','BUD') as fxID")
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
  cutoff_value = get_incr_col_max_value(fx_rates)
  update_cutoff_value(cutoff_value, table_name, 'edmftp.fx_budget_rates')
  update_run_datetime(run_datetime, table_name, 'edmftp.fx_budget_rates')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core
