# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.country

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'sapp01.t005t', prune_days)
  main_inc = load_incr_dataset('sapp01.t005t', '_modified', cutoff_value)
else:
  main_inc = load_full_dataset('sapp01.t005t')

# COMMAND ----------

# SAMPLING
if sampling:
  main_inc = main_inc.limit(10)

# COMMAND ----------

# VIEWS
main_inc.createOrReplaceTempView('t005t')

# COMMAND ----------

main = spark.sql("""Select distinct
                    NULL                                    AS createdBy,
                    NULL                                    AS createdOn,
                    NULL                                    AS modifiedBy,
                    NULL                                    AS modifiedOn,
                    CURRENT_TIMESTAMP()                     AS insertedOn,
                    CURRENT_TIMESTAMP()                     AS updatedOn,
                    T005T.LAND1                             AS countryCode,
                    T005T.LANDX                             AS countryDescription,
                    T005T.LANDX                             AS countryShortName,
                    T005T.LAND1                             AS euCode,
                    spt.ForecastArea                        AS forecastArea,
                    NULL                                    AS isoCode,
                    NULL                                    AS isoCodeNumeric,
                    spt.ManagementArea                      AS managementArea,
                    NULL                                    AS nlsCode,
                    'FALSE'                                 AS obsoleteFlag,
                    spt.Region                              AS region,
                    spt.subRegion                           AS subRegion,
                    spt.CorporatesubRegion                  AS subRegionGis
                    from T005T 
                    Left Join spt.E2EcountriesMasterTable spt ON T005T.LAND1 = spt.country
                    Where T005T.SPRAS='E'""")
                    
main.cache()
display(main)

# COMMAND ----------

main = remove_duplicate_rows(df = main, 
                             key_columns = ['countryCode'], 
                             tableName = table_name, 
                             sourceName = source_name, 
                             notebookName = NOTEBOOK_NAME, 
                             notebookPath = NOTEBOOK_PATH)

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .transform(tg_default(source_name))
  .transform(tg_core_country())
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
  spark.sql("""
            SELECT
                T005T.LAND1 AS countryCode
                from sapp01.t005t 
                Where T005T.SPRAS='E'
            """)
  .transform(attach_source_column(source = source_name))
  .transform(attach_primary_key('countryCode,_SOURCE', 'edm.country'))
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
  cutoff_value = get_incr_col_max_value(main_inc, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 'sapp01.t005t ')
  update_run_datetime(run_datetime, table_name, 'sapp01.t005t ')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core
