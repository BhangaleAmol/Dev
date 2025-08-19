# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.organization

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'ebs.hr_all_organization_units', prune_days)
  main_inc = load_incr_dataset('ebs.hr_all_organization_units', 'LAST_UPDATE_DATE', cutoff_value)
else:
  main_inc = load_full_dataset('ebs.hr_all_organization_units')

# COMMAND ----------

# SAMPLING
if sampling:
  main_inc = main_inc.limit(10)

# COMMAND ----------

# VIEWS
main_inc.createOrReplaceTempView('main_inc')

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'EBS.XLE_ENTITY_PROFILES', prune_days)
  main_inccompany = load_incr_dataset('EBS.XLE_ENTITY_PROFILES', 'LAST_UPDATE_DATE', cutoff_value)
else:
  main_inccompany = load_full_dataset('EBS.XLE_ENTITY_PROFILES')

# COMMAND ----------

# SAMPLING
if sampling:
  main_inccompany = main_inccompany.limit(10)

# COMMAND ----------

# VIEWS
main_inccompany.createOrReplaceTempView('main_inccompany')

# COMMAND ----------

TMP_LEGAL_ENTITY = spark.sql("""
SELECT
    XEP.LEGAL_ENTITY_ID ,
    XEP.NAME ,
    HR_OUTL.NAME ORGANIZATION_NAME,
    HR_OUTL.ORGANIZATION_ID ,
    HR_LOC.LOCATION_ID ,
    HR_LOC.COUNTRY ,
    HR_LOC.LOCATION_CODE ,
    GLEV.FLEX_SEGMENT_VALUE LEGAL_ENTITY_CODE
FROM
    EBS.XLE_ENTITY_PROFILES XEP,
    EBS.XLE_REGISTRATIONS REG,
    EBS.HR_OPERATING_UNITS HOU,
    EBS.HR_ALL_ORGANIZATION_UNITS HR_OUTL,
    EBS.HR_LOCATIONS_ALL HR_LOC,
    EBS.GL_LEGAL_ENTITIES_BSVS GLEV
WHERE
    1 = 1
    AND   XEP.TRANSACTING_ENTITY_FLAG = 'Y'
    AND   XEP.LEGAL_ENTITY_ID = REG.SOURCE_ID
    AND   REG.SOURCE_TABLE = 'XLE_ENTITY_PROFILES'
    AND   REG.IDENTIFYING_FLAG = 'Y'
    AND   XEP.LEGAL_ENTITY_ID = HOU.DEFAULT_LEGAL_CONTEXT_ID
    AND   REG.LOCATION_ID = HR_LOC.LOCATION_ID
    AND   XEP.LEGAL_ENTITY_ID = GLEV.LEGAL_ENTITY_ID
    AND   HR_OUTL.ORGANIZATION_ID = HOU.ORGANIZATION_ID
     -- exclude some exotic and duplicate legal entities
    AND   GLEV.FLEX_SEGMENT_VALUE NOT IN ('5120', '2011', '5110' )
    AND   HR_OUTL.ORGANIZATION_ID NOT IN (977, 978, 1011)
""")

TMP_LEGAL_ENTITY.createOrReplaceTempView("TMP_LEGAL_ENTITY")
TMP_LEGAL_ENTITY.cache()
TMP_LEGAL_ENTITY.count()

display(TMP_LEGAL_ENTITY)

# COMMAND ----------

qv_region_dc_master=spark.sql("""
select
  Globallocationcode,
  ERPWHCode,
  WHorDSSelection,
  SubRegion,
  Region,
  Description,
  _deleted
from
  qv.region_dc_master
union
select distinct
  'DSAPAC' as Globallocationcode,
  '325' as ERPWHCode,
  'DS' as WHorDSSelection,
  'APAC' as SubRegion,
  'APAC' as Region,
  'APAC Virtual DS DS01' as Description,
  false as _deleted
from
  qv.region_dc_master
where
  not exists (select 1 from qv.region_dc_master where ERPWHCode = '325')
""")
qv_region_dc_master.createOrReplaceTempView('qv_region_dc_master')

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   Globallocationcode,
# MAGIC   ERPWHCode,
# MAGIC   WHorDSSelection,
# MAGIC   SubRegion,
# MAGIC   Region,
# MAGIC   Description,
# MAGIC   _deleted
# MAGIC from
# MAGIC   qv.region_dc_master
# MAGIC union
# MAGIC select distinct
# MAGIC   'DSAPAC' as Globallocationcode,
# MAGIC   '325' as ERPWHCode,
# MAGIC   'DS' as WHorDSSelection,
# MAGIC   'APAC' as SubRegion,
# MAGIC   'APAC' as Region,
# MAGIC   'APAC Virtual DS DS01' as Description,
# MAGIC   false as _deleted
# MAGIC from
# MAGIC   qv.region_dc_master
# MAGIC where
# MAGIC   not exists (select 1 from qv.region_dc_master where ERPWHCode = '325')

# COMMAND ----------

main_inv = spark.sql("""
SELECT 
CASE
    WHEN HAOU.CREATED_BY > 0 THEN REPLACE(STRING(INT (HAOU.CREATED_BY)), ",", "")
  END createdBy,
  HAOU.CREATION_DATE createdOn,
  CASE
    WHEN HAOU.LAST_UPDATED_BY > 0 THEN REPLACE(
      STRING(INT (HAOU.LAST_UPDATED_BY)),
      ",",
      ""
    )
  END modifiedBy,
  HAOU.LAST_UPDATE_DATE modifiedOn,
  CURRENT_TIMESTAMP insertedOn,
  CURRENT_TIMESTAMP updatedOn,
  HLA.ADDRESS_LINE_1 AS addressLine1,
  HLA.ADDRESS_LINE_2 AS addressLine2,
  HLA.ADDRESS_LINE_3 AS addressLine3,
  HLA.TOWN_OR_CITY AS city,
  '' commonName,
  'DC' || MTL_PARAMETERS.ORGANIZATION_CODE commonOrganizationCode,
  HLA.COUNTRY AS country,
  HLA.COUNTRY AS countryCode,
  GL.CURRENCY_CODE currency,
  CASE
    WHEN DATE_FORMAT(CURRENT_DATE, 'yyyyMMdd') BETWEEN DATE_FORMAT(HAOU.DATE_FROM, 'yyyyMMdd')
    AND COALESCE(
      DATE_FORMAT(HAOU.DATE_TO, 'yyyyMMdd'),
      DATE_FORMAT(CURRENT_DATE, 'yyyyMMdd')
    ) THEN TRUE
    ELSE FALSE
  END isActive,
  HAOU.NAME name,
  MTL_PARAMETERS.ORGANIZATION_CODE organizationCode,
  REPLACE(
    STRING(INT (HAOU.ORGANIZATION_ID)),
    ",",
    ""
  ) organizationId,  
  'INV' organizationType,
  HLA.POSTAL_CODE AS postalCode,
  qv_region_dc_master.region,
  qv_region_dc_master.subRegion,
  HLA.REGION_2 AS state
FROM 
    main_inc HAOU
    LEFT JOIN EBS.HR_ORGANIZATION_INFORMATION HOI1
        ON HAOU.ORGANIZATION_ID = HOI1.ORGANIZATION_ID
    LEFT JOIN EBS.GL_LEDGERS GL
        ON HOI1.ORG_INFORMATION1 = GL.LEDGER_ID 
    LEFT JOIN EBS.MTL_PARAMETERS ON HAOU.ORGANIZATION_ID = MTL_PARAMETERS.ORGANIZATION_ID
    LEFT JOIN EBS.HR_LOCATIONS_ALL HLA ON HAOU.LOCATION_ID = HLA.LOCATION_ID 
    LEFT join qv_region_dc_master
      on qv_region_dc_master.ERPWHCode = MTL_PARAMETERS.ORGANIZATION_CODE
WHERE HOI1.ORG_INFORMATION_CONTEXT = 'Accounting Information'
""")

main_inv.cache()
display(main_inv)

# COMMAND ----------

main_comp = spark.sql("""
SELECT 
CASE
    WHEN HAOU.CREATED_BY > 0 THEN REPLACE(STRING(INT (HAOU.CREATED_BY)), ",", "")
  END createdBy,
  HAOU.CREATION_DATE createdOn,
  CASE
    WHEN HAOU.LAST_UPDATED_BY > 0 THEN REPLACE(
      STRING(INT (HAOU.LAST_UPDATED_BY)),
      ",",
      ""
    )
  END modifiedBy,
  HAOU.LAST_UPDATE_DATE modifiedOn,
  CURRENT_TIMESTAMP insertedOn,
  CURRENT_TIMESTAMP updatedOn,
  CAST(NULL AS STRING) AS addressLine1,
  CAST(NULL AS STRING) AS addressLine2,
  CAST(NULL AS STRING) AS addressLine3,
  CAST(NULL AS STRING) AS city,
  '' commonName,
  '' commonOrganizationCode,
  CAST(NULL AS STRING) AS country,
  CAST(NULL AS STRING) AS countryCode,
  Null currency,
  CASE
    WHEN DATE_FORMAT(CURRENT_DATE, 'yyyyMMdd') BETWEEN DATE_FORMAT(HAOU.DATE_FROM, 'yyyyMMdd')
    AND COALESCE(
      DATE_FORMAT(HAOU.DATE_TO, 'yyyyMMdd'),
      DATE_FORMAT(CURRENT_DATE, 'yyyyMMdd')
    ) THEN TRUE
    ELSE FALSE
  END isActive,
  HAOU.NAME name,
  TMP_LEGAL_ENTITY.LEGAL_ENTITY_CODE organizationCode,
  REPLACE(
    STRING(INT (HAOU.ORGANIZATION_ID)),
    ",",
    ""
  ) organizationId,
  HOI1.ORG_INFORMATION1 organizationType,
  CAST(NULL AS STRING) AS postalCode,
  region_dc_master.region,
  region_dc_master.subRegion,
  CAST(NULL AS STRING) AS state
FROM 
    main_inc HAOU
    LEFT JOIN EBS.HR_ORGANIZATION_INFORMATION HOI1
        ON HAOU.ORGANIZATION_ID = HOI1.ORGANIZATION_ID
    LEFT JOIN TMP_LEGAL_ENTITY ON HAOU.ORGANIZATION_ID = TMP_LEGAL_ENTITY.ORGANIZATION_ID
    LEFT join qv.region_dc_master
      on region_dc_master.ERPWHCode = REPLACE(STRING(INT (HAOU.ORGANIZATION_ID)), ",", "" ) 
WHERE HOI1.ORG_INFORMATION_CONTEXT = 'CLASS'
AND HOI1.ORG_INFORMATION1 = 'OPERATING_UNIT'
and TMP_LEGAL_ENTITY.LEGAL_ENTITY_CODE <> '7250'
""")

main_comp.cache()
display(main_comp)

# COMMAND ----------

main_company = spark.sql("""
SELECT 
CASE
    WHEN CMPNY.CREATED_BY > 0 THEN REPLACE(STRING(INT (CMPNY.CREATED_BY)), ",", "")
  END createdBy,
  CMPNY.CREATION_DATE createdOn,
  CASE
    WHEN CMPNY.LAST_UPDATED_BY > 0 THEN REPLACE(
      STRING(INT (CMPNY.LAST_UPDATED_BY)),
      ",",
      ""
    )
  END modifiedBy,
  CMPNY.LAST_UPDATE_DATE modifiedOn,
  CURRENT_TIMESTAMP insertedOn,
  CURRENT_TIMESTAMP updatedOn,
  CAST(NULL AS STRING) AS addressLine1,
  CAST(NULL AS STRING) AS addressLine2,
  CAST(NULL AS STRING) AS addressLine3,
  CAST(NULL AS STRING) AS city,
  CAST(NULL AS STRING) AS commonName,
  CAST(NULL AS STRING) AS commonOrganizationCode,
  CAST(NULL AS STRING) AS country,
  CAST(NULL AS STRING) AS countryCode,
  CAST(NULL AS STRING) AS currency,
  TRUE isActive,
  CMPNY.NAME AS name,
  CAST(NULL AS STRING) AS organizationCode,
  'COMPANY'||'-'||Replace(STRING(INT(CMPNY.LEGAL_ENTITY_ID)), ",", "")  As organizationId,  
  'COMPANY' organizationType,
  CAST(NULL AS STRING) AS postalCode,
  region_dc_master.region,
  region_dc_master.subRegion,
  CAST(NULL AS STRING) AS state
  FROM main_inccompany CMPNY
  LEFT join qv.region_dc_master
      on region_dc_master.ERPWHCode = concat('COMPANY', '-', Replace(STRING(INT(CMPNY.LEGAL_ENTITY_ID)), ",", ""))
  """)
                
main_company.cache()
display(main_company)


# COMMAND ----------

main = main_inv.unionAll(main_comp).unionAll(main_company)
main.cache()

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

main = remove_duplicate_rows(df = main, 
                             key_columns = ['organizationId'], 
                             tableName = table_name, 
                             sourceName = source_name, 
                             notebookName = NOTEBOOK_NAME, 
                             notebookPath = NOTEBOOK_PATH)

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .transform(tg_default(source_name))
  .transform(tg_core_organization())
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
full_ou_keys = spark.sql("""
  SELECT
    REPLACE(STRING(INT (HAOU.ORGANIZATION_ID)), ",", "") organizationId
  FROM ebs.hr_all_organization_units HAOU
  LEFT JOIN EBS.HR_ORGANIZATION_INFORMATION HOI1 ON HAOU.ORGANIZATION_ID = HOI1.ORGANIZATION_ID
  WHERE 
    HOI1.ORG_INFORMATION_CONTEXT = 'CLASS'
    AND HOI1.ORG_INFORMATION1 = 'OPERATING_UNIT'
""")

full_ai_keys = spark.sql("""
  SELECT
    REPLACE(STRING(INT (HAOU.ORGANIZATION_ID)), ",", "") organizationId
  FROM ebs.hr_all_organization_units HAOU
  LEFT JOIN EBS.HR_ORGANIZATION_INFORMATION HOI1 ON HAOU.ORGANIZATION_ID = HOI1.ORGANIZATION_ID
  WHERE 
    HOI1.ORG_INFORMATION_CONTEXT = 'Accounting Information'
""")

full_company_keys = spark.sql("""
  SELECT
   'COMPANY'||'-'||Replace(STRING(INT(CMPNY.LEGAL_ENTITY_ID)), ",", "")  As organizationId
  FROM EBS.XLE_ENTITY_PROFILES CMPNY
""")
full_keys_f = (
  full_ai_keys
  .unionAll(full_ou_keys)
  .unionAll(full_company_keys)
  .transform(attach_source_column(source = source_name))
  .transform(attach_surrogate_key(columns = 'organizationId,_SOURCE'))
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
  cutoff_value = get_incr_col_max_value(main_inc,'LAST_UPDATE_DATE')
  update_cutoff_value(cutoff_value, table_name, 'ebs.hr_all_organization_units')
  update_run_datetime(run_datetime, table_name, 'ebs.hr_all_organization_units')
  
  cutoff_value = get_incr_col_max_value(main_inccompany,'LAST_UPDATE_DATE')
  update_cutoff_value(cutoff_value, table_name, 'EBS.XLE_ENTITY_PROFILES')
  update_run_datetime(run_datetime, table_name, 'EBS.XLE_ENTITY_PROFILES')  

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core
