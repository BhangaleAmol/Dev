# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.ledger_ebs

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'EBS.GL_LEDGERS', prune_days)
  main_inc = load_incr_dataset('EBS.GL_LEDGERS', 'LAST_UPDATE_DATE', cutoff_value)
else:
  main_inc = load_full_dataset('EBS.GL_LEDGERS')

# COMMAND ----------

# SAMPLING
if sampling:
  main_inc = main_inc.limit(10)

# COMMAND ----------

# VIEWS
main_inc.createOrReplaceTempView('GL_LEDGERS')

# COMMAND ----------

main = spark.sql("""
SELECT 
    REPLACE(STRING(INT (GL_LEDGERS.CREATED_BY)),
    ",",
    ""
     ) AS createdBy,
      GL_LEDGERS.CREATION_DATE AS createdOn,
     REPLACE(STRING(INT (GL_LEDGERS.LAST_UPDATED_BY )),
    ",",
    ""
    ) AS modifiedBy,
    GL_LEDGERS.LAST_UPDATE_DATE AS modifiedOn,
    CURRENT_TIMESTAMP() AS insertedOn,
    CURRENT_TIMESTAMP() AS updatedOn,
    GL_LEDGERS.PERIOD_SET_NAME AS calendarName,
    REPLACE(STRING(INT (GL_LEDGERS.CHART_OF_ACCOUNTS_ID)),
    ",",
    ""
    ) AS chartOfAccounts,
    GL_LEDGERS.CURRENCY_CODE AS currencyCode,
    GL_LEDGERS.LEDGER_CATEGORY_CODE AS ledgerCategoryCode,
    NULL AS ledgerCategoryName,
    REPLACE(STRING(INT (GL_LEDGERS.LEDGER_ID)),
    ",",
    ""
    ) AS ledgerId,
    GL_LEDGERS.NAME AS name,
    GL_LEDGERS.SHORT_NAME AS shortName
   FROM GL_LEDGERS
 """)

main.createOrReplaceTempView('main')
main.cache()
display(main)
    

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

main = remove_duplicate_rows(df = main, 
                             key_columns = ['ledgerId'], 
                             tableName = table_name, 
                             sourceName = source_name, 
                             notebookName = NOTEBOOK_NAME, 
                             notebookPath = NOTEBOOK_PATH)

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .transform(tg_default(source_name))
  .transform(tg_core_ledger())
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
  spark.sql("""select REPLACE(STRING(INT (GL_LEDGERS.LEDGER_ID)),
              ",",
                ""
                ) AS ledgerId 
                from ebs.gl_ledgers""")
  .transform(attach_source_column(source = source_name))
  .transform(attach_surrogate_key(columns = 'ledgerId,_SOURCE'))
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
  update_cutoff_value(cutoff_value, table_name, 'ebs.GL_LEDGERS')
  update_run_datetime(run_datetime, table_name, 'ebs.GL_LEDGERS')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core
