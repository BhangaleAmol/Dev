# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.territory

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

# LOAD DATASETS
main_inc = load_full_dataset('spt.E2EcountriesMasterTable')

# COMMAND ----------

# SAMPLING
if sampling:
  main_inc = main_inc.limit(10)

# COMMAND ----------

# VIEWS
main_inc.createOrReplaceTempView('main_inc')

# COMMAND ----------

main = spark.sql("""Select distinct
                    CAST(NULL AS STRING) AS createdBy,
                    CAST(NULL AS TIMESTAMP)  AS createdOn,
                    CAST(NULL AS STRING) AS modifiedBy,
                    CAST(NULL AS TIMESTAMP)  AS modifiedOn,
                    CURRENT_TIMESTAMP() AS insertedOn,
                    CURRENT_TIMESTAMP() AS updatedOn,
                    Country AS territoryCode,
                    CountryName AS territoryShortName,
                    CountryName AS territoryDescription,
                    'BR' AS euCode,
                    subRegion AS subRegion,
                    CorporatesubRegion AS subRegionGis,
                    ManagementArea AS managementArea,
                    Region AS region,
                    ForecastArea AS forecastArea,
                    False AS obsoleteFlag,
                    '076' AS isoCodeNumeric,
                    'BRA' AS isoCode,
                    'BRAZIL' AS nlsCode
                    from  
                    main_inc
                    WHERE Country='BR'""")
main.cache()
display(main)

# COMMAND ----------

main = remove_duplicate_rows(df = main, 
                             key_columns = ['territoryCode'], 
                             tableName = table_name, 
                             sourceName = source_name, 
                             notebookName = NOTEBOOK_NAME, 
                             notebookPath = NOTEBOOK_PATH)

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .transform(tg_default(source_name))
  .transform(tg_core_territory())
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
  spark.sql("""select Country AS territoryCode 
              from  
                    spt.E2EcountriesMasterTable 
              WHERE Country='BR'
            """)
  .transform(attach_source_column(source = source_name))
  .transform(attach_surrogate_key(columns = 'territoryCode,_SOURCE'))
  .select('_ID')
  .transform(add_unknown_ID())
)

apply_soft_delete(full_keys_f, table_name, key_columns = '_ID')

# COMMAND ----------

# UPDATE FK
update_foreign_key(table_name, 'createdBy,_SOURCE', 'createdBy_ID', 'edm.user')
update_foreign_key(table_name, 'modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core
