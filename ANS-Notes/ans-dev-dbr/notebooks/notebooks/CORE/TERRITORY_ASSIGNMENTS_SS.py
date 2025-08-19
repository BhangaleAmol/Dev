# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.territory_assignments

# COMMAND ----------

# LOAD DATASETS
qvpos_sales_reporting_org_structure = load_full_dataset('smartsheets.qvpos_sales_reporting_org_structure')
qvpos_sales_reporting_org_structure.createOrReplaceTempView('qvpos_sales_reporting_org_structure')

qvpos_sales_reporting_2nd_org_structure = load_full_dataset('smartsheets.qvpos_sales_reporting_2nd_org_structure')
qvpos_sales_reporting_2nd_org_structure.createOrReplaceTempView('qvpos_sales_reporting_2nd_org_structure')

# COMMAND ----------

# SAMPLING
if sampling:
  qvpos_sales_reporting_org_structure = qvpos_sales_reporting_org_structure.limit(10)
  qvpos_sales_reporting_org_structure.createOrReplaceTempView('qvpos_sales_reporting_org_structure')
  
  qvpos_sales_reporting_2nd_org_structure = qvpos_sales_reporting_2nd_org_structure.limit(10)
  qvpos_sales_reporting_2nd_org_structure.createOrReplaceTempView('qvpos_sales_reporting_2nd_org_structure')

# COMMAND ----------

sales_org = spark.sql("""
select
  rowNumber,
  companyVisibility,
  region,
  salesOrg,
  territory,
  user,
  trim(vertical) vertical
from
  (select
  sros.rowNumber,
  sros.companyVisibility,
  sros.region,
  sros.salesOrg,
  nvl(sros.territory, 'BLANK') territory,
  sros.user,
  explode(split(sros.verticals, ',')) vertical
from
  smartsheets.qvpos_sales_reporting_org_structure sros
 where not sros._DELETED)
""")
sales_org.count()
sales_org=sales_org.createOrReplaceTempView('sales_org')

# COMMAND ----------

TMP_MED_ACUTE = spark.sql("""
select
  distinct
  0 createdBy,
  TO_TIMESTAMP(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS') AS createdOn,
  0 modifiedBy,
  TO_TIMESTAMP(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS') AS modifiedOn,
  current_date insertedOn,
  current_date updatedOn,
  salesOrg salesOrganization,
  territory territoryID,
  division,
  zip3,
  vertical,
  salesRegion,
  userId,
  'DEFAULT' scenario
from
      (select
      sales_org.salesOrg,
      sales_org.territory,
      'MEDICAL' division,
      left(zipcodes.3DigitZip,3) zip3,
      sales_org.vertical,
      sales_org.region salesRegion,
      sales_org.user userId,
      sales_org.companyVisibility,
      'ACUTE' sourceFile
    from
      sales_org
      JOIN smartsheets.qvpos_acute_org_zip zipcodes on sales_org.territory = nvl(zipcodes.territory, 'BLANK')
    where
      zipcodes.3DigitZip is not null
 and not zipcodes._DELETED)
 """)
TMP_MED_ACUTE.count()
TMP_MED_ACUTE=TMP_MED_ACUTE.createOrReplaceTempView('TMP_MED_ACUTE')

# COMMAND ----------

TMP_IND_AUTO = spark.sql("""
select
  distinct
  0 createdBy,
  TO_TIMESTAMP(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS') AS createdOn,
  0 modifiedBy,
  TO_TIMESTAMP(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS') AS modifiedOn,
  current_date insertedOn,
  current_date updatedOn,
  salesOrg salesOrganization,
  territory territoryID,
  division,
  zip3,
  vertical,
  salesRegion,
  userId,
  'DEFAULT' scenario
from(select
      sales_org.salesOrg,
      sales_org.territory,
      'INDUSTRIAL' division,
      left(zipcodes.3DigitZip,3) zip3,
      sales_org.vertical,
      sales_org.region salesRegion,
      sales_org.user userId,
      sales_org.companyVisibility,
      'AUTO' sourceFile
    from
      sales_org
      JOIN smartsheets.qvpos_auto_org_zip zipcodes on sales_org.territory = nvl(zipcodes.territory, 'BLANK')
    where
      zipcodes.3DigitZip is not null
 and not zipcodes._DELETED)
 """)
TMP_IND_AUTO.count()
TMP_IND_AUTO=TMP_IND_AUTO.createOrReplaceTempView('TMP_IND_AUTO')

# COMMAND ----------

TMP_MED_EMS = spark.sql("""
select
  distinct
  0 createdBy,
  TO_TIMESTAMP(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS') AS createdOn,
  0 modifiedBy,
  TO_TIMESTAMP(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS') AS modifiedOn,
  current_date insertedOn,
  current_date updatedOn,
  salesOrg salesOrganization,
  territory territoryID,
  division,
  zip3,
  vertical,
  salesRegion,
  userId,
  'DEFAULT' scenario
from(select
      sales_org.salesOrg,
      sales_org.territory,
      'MEDICAL' division,
      zipcodes.3DigitZip zip3,
      sales_org.vertical,
      sales_org.region salesRegion,
      sales_org.user userId,
      sales_org.companyVisibility,
      'EMS' sourceFile
    from
      sales_org
      JOIN smartsheets.qvpos_alt_ems_org_zip zipcodes on sales_org.territory = nvl(zipcodes.territory, 'BLANK')
    where
      zipcodes.3DigitZip is not null
 and not zipcodes._DELETED)
 """)
TMP_MED_EMS.count()
TMP_MED_EMS=TMP_MED_EMS.createOrReplaceTempView('TMP_MED_EMS')

# COMMAND ----------

TMP_MED_DENTAL = spark.sql("""
select
  distinct
  0 createdBy,
  TO_TIMESTAMP(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS') AS createdOn,
  0 modifiedBy,
  TO_TIMESTAMP(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS') AS modifiedOn,
  current_date insertedOn,
  current_date updatedOn,
  salesOrg salesOrganization,
  territory territoryID,
  division,
  zip3,
  vertical,
  salesRegion,
  userId,
  'DEFAULT' scenario
from(select
      sales_org.salesOrg,
      sales_org.territory,
      'MEDICAL' division,
      left(zipcodes.3DigitZip,3) zip3,
      sales_org.vertical,
      sales_org.region salesRegion,
      sales_org.user userId,
      sales_org.companyVisibility,
      'DENTAL' sourceFile
    from
      sales_org
      JOIN smartsheets.qvpos_dental_org_zip zipcodes on sales_org.territory = nvl(zipcodes.territory, 'BLANK')
    where
      zipcodes.3DigitZip is not null
 and not zipcodes._DELETED)
 """)
TMP_MED_DENTAL.count()
TMP_MED_DENTAL=TMP_MED_DENTAL.createOrReplaceTempView('TMP_MED_DENTAL')

# COMMAND ----------

TMP_IND_IND = spark.sql("""
select
  distinct
  0 createdBy,
  TO_TIMESTAMP(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS') AS createdOn,
  0 modifiedBy,
  TO_TIMESTAMP(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS') AS modifiedOn,
  current_date insertedOn,
  current_date updatedOn,
  salesOrg salesOrganization,
  territory territoryID,
  division,
  zip3,
  vertical,
  salesRegion,
  userId,
  'DEFAULT' scenario
from(select
      sales_org.salesOrg,
      sales_org.territory,
      'INDUSTRIAL' division,
      left(zipcodes.3DigitZip,3) zip3,
      sales_org.vertical,
      sales_org.region salesRegion,
      sales_org.user userId,
      sales_org.companyVisibility,
      'IND' sourceFile
    from
      sales_org
      JOIN smartsheets.qvpos_ind_org_zip zipcodes on sales_org.territory = nvl(zipcodes.territory, 'BLANK')
    where
      zipcodes.3DigitZip is not null
 and not zipcodes._DELETED)
 """)
TMP_IND_IND.count()
TMP_IND_IND=TMP_IND_IND.createOrReplaceTempView('TMP_IND_IND')


# COMMAND ----------

TMP_MED_HSS = spark.sql("""
select
  distinct
  0 createdBy,
  TO_TIMESTAMP(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS') AS createdOn,
  0 modifiedBy,
  TO_TIMESTAMP(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS') AS modifiedOn,
  current_date insertedOn,
  current_date updatedOn,
  salesOrg salesOrganization,
  territory territoryID,
  division,
  zip3,
  vertical,
  salesRegion,
  userId,
  'DEFAULT' scenario
from(select
      sales_org.salesOrg,
      sales_org.territory,
      'MEDICAL' division,
      left(zipcodes.3DigitZip,3) zip3,
      sales_org.vertical,
      sales_org.region salesRegion,
      sales_org.user userId,
      sales_org.companyVisibility,
      'HSS' sourceFile
    from
      sales_org
      JOIN smartsheets.qvpos_hss_org_zip zipcodes on sales_org.territory = nvl(zipcodes.territory, 'BLANK')
    where
      zipcodes.3DigitZip is not null
 and not zipcodes._DELETED)
 """)
TMP_MED_HSS.count()
TMP_MED_HSS=TMP_MED_HSS.createOrReplaceTempView('TMP_MED_HSS')

# COMMAND ----------

sales_org_default = spark.sql("""
select * from TMP_MED_ACUTE
UNION
select * from TMP_IND_AUTO
UNION
select * from TMP_MED_EMS
UNION
select * from TMP_MED_DENTAL
UNION
select * from TMP_IND_IND
UNION
select * from TMP_MED_HSS
""")

# COMMAND ----------

# DUPLICATES
sales_org_default_columns = ['vertical', 'zip3', 'scenario']

duplicates = get_duplicate_rows(sales_org_default, sales_org_default_columns)
duplicates.cache()

if not duplicates.rdd.isEmpty():
  notebook_data = {
    'source_name': 'CORE',
    'notebook_name': NOTEBOOK_NAME,
    'notebook_path': NOTEBOOK_PATH,
    'target_name': table_name,
    'duplicates_count': duplicates.count(),
    'duplicates_sample': duplicates.select(sales_org_default_columns).limit(50)
  }
  send_mail_duplicate_records_found(notebook_data)
  
sales_org_default_f = sales_org_default.dropDuplicates(sales_org_default_columns)
sales_org_default_f.createOrReplaceTempView('sales_org_default_f')
sales_org_default_f.display()

# COMMAND ----------

# ORG2 :
sales_org = spark.sql("""
select
  rowNumber,
  companyVisibility,
  region,
  salesOrg,
  territory,
  user,
  trim(vertical) vertical
from
  (select
  sros.rowNumber,
  sros.companyVisibility,
  sros.region,
  sros.salesOrg,
  nvl(sros.territory, 'BLANK') territory,
  sros.user,
  explode(split(sros.verticals, ',')) vertical
from
  smartsheets.qvpos_sales_reporting_2nd_org_structure sros
 where not sros._DELETED)
""")
sales_org.count()
sales_org=sales_org.createOrReplaceTempView('sales_org')

# COMMAND ----------

TMP_ORG2_MED_ACUTE = spark.sql("""
select
  distinct
  0 createdBy,
  TO_TIMESTAMP(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS') AS createdOn,
  0 modifiedBy,
  TO_TIMESTAMP(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS') AS modifiedOn,
  current_date insertedOn,
  current_date updatedOn,
  salesOrg salesOrganization,
  territory territoryID,
  division,
  zip3,
  vertical,
  salesRegion,
  userId,
  'ORG2' scenario
from
      (select
      sales_org.salesOrg,
      sales_org.territory,
      'MEDICAL' division,
      left(zipcodes.3DigitZip,3) zip3,
      sales_org.vertical,
      sales_org.region salesRegion,
      sales_org.user userId,
      sales_org.companyVisibility,
      'ACUTE' sourceFile
    from
      sales_org
      JOIN smartsheets.qvpos_acute_org_zip zipcodes on sales_org.territory = nvl(zipcodes.2nd_Territory, 'BLANK')
    where
      zipcodes.3DigitZip is not null
       and not zipcodes._DELETED
      and sales_org.territory not in ('BLANK'))
 """)
TMP_ORG2_MED_ACUTE.count()
TMP_ORG2_MED_ACUTE=TMP_ORG2_MED_ACUTE.createOrReplaceTempView('TMP_ORG2_MED_ACUTE')

# COMMAND ----------

TMP_ORG2_IND_AUTO = spark.sql("""
select
  distinct
  0 createdBy,
  TO_TIMESTAMP(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS') AS createdOn,
  0 modifiedBy,
  TO_TIMESTAMP(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS') AS modifiedOn,
  current_date insertedOn,
  current_date updatedOn,
  salesOrg salesOrganization,
  territory territoryID,
  division,
  zip3,
  vertical,
  salesRegion,
  userId,
  'ORG2' scenario
from
      (select
      sales_org.salesOrg,
      sales_org.territory,
      'INDUSTRIAL' division,
      left(zipcodes.3DigitZip,3) zip3,
      sales_org.vertical,
      sales_org.region salesRegion,
      sales_org.user userId,
      sales_org.companyVisibility,
      'AUTO' sourceFile
    from
      sales_org
      JOIN smartsheets.qvpos_auto_org_zip zipcodes on sales_org.territory = nvl(zipcodes.2nd_Territory, 'BLANK')
    where
      zipcodes.3DigitZip is not null
       and not zipcodes._DELETED
      and sales_org.territory not in ('BLANK'))
 """)
TMP_ORG2_IND_AUTO.count()
TMP_ORG2_IND_AUTO=TMP_ORG2_IND_AUTO.createOrReplaceTempView('TMP_ORG2_IND_AUTO')

# COMMAND ----------

TMP_ORG2_MED_EMS = spark.sql("""
select
  distinct
  0 createdBy,
  TO_TIMESTAMP(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS') AS createdOn,
  0 modifiedBy,
  TO_TIMESTAMP(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS') AS modifiedOn,
  current_date insertedOn,
  current_date updatedOn,
  salesOrg salesOrganization,
  territory territoryID,
  division,
  zip3,
  vertical,
  salesRegion,
  userId,
  'ORG2' scenario
from
      (select
      sales_org.salesOrg,
      sales_org.territory,
      'MEDICAL' division,
      zipcodes.3DigitZip zip3,
      sales_org.vertical,
      sales_org.region salesRegion,
      sales_org.user userId,
      sales_org.companyVisibility,
      'EMS' sourceFile
    from
      sales_org
      JOIN smartsheets.qvpos_alt_ems_org_zip zipcodes on sales_org.territory = nvl(zipcodes.2nd_Territory, 'BLANK')
    where
      zipcodes.3DigitZip is not null
       and not zipcodes._DELETED
      and sales_org.territory not in ('BLANK'))
 """)
TMP_ORG2_MED_EMS.count()
TMP_ORG2_MED_EMS=TMP_ORG2_MED_EMS.createOrReplaceTempView('TMP_ORG2_MED_EMS')

# COMMAND ----------

TMP_ORG2_MED_DENTAL = spark.sql("""
select
  distinct
  0 createdBy,
  TO_TIMESTAMP(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS') AS createdOn,
  0 modifiedBy,
  TO_TIMESTAMP(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS') AS modifiedOn,
  current_date insertedOn,
  current_date updatedOn,
  salesOrg salesOrganization,
  territory territoryID,
  division,
  zip3,
  vertical,
  salesRegion,
  userId,
  'ORG2' scenario
from
      (select
      sales_org.salesOrg,
      sales_org.territory,
      'MEDICAL' division,
      left(zipcodes.3DigitZip,3) zip3,
      sales_org.vertical,
      sales_org.region salesRegion,
      sales_org.user userId,
      sales_org.companyVisibility,
      'DENTAL' sourceFile
    from
      sales_org
      JOIN smartsheets.qvpos_dental_org_zip zipcodes on sales_org.territory = nvl(zipcodes.2nd_Territory, 'BLANK')
    where
      zipcodes.3DigitZip is not null
       and not zipcodes._DELETED
      and sales_org.territory not in ('BLANK'))
 """)
TMP_ORG2_MED_DENTAL.count()
TMP_ORG2_MED_DENTAL=TMP_ORG2_MED_DENTAL.createOrReplaceTempView('TMP_ORG2_MED_DENTAL')

# COMMAND ----------

TMP_ORG2_IND_IND = spark.sql("""
select
  distinct
  0 createdBy,
  TO_TIMESTAMP(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS') AS createdOn,
  0 modifiedBy,
  TO_TIMESTAMP(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS') AS modifiedOn,
  current_date insertedOn,
  current_date updatedOn,
  salesOrg salesOrganization,
  territory territoryID,
  division,
  zip3,
  vertical,
  salesRegion,
  userId,
  'ORG2' scenario
from
      (select
      sales_org.salesOrg,
      sales_org.territory,
      'INDUSTRIAL' division,
      left(zipcodes.3DigitZip,3) zip3,
      sales_org.vertical,
      sales_org.region salesRegion,
      sales_org.user userId,
      sales_org.companyVisibility,
      'IND' sourceFile
    from
      sales_org
      JOIN smartsheets.qvpos_ind_org_zip zipcodes on sales_org.territory = nvl(zipcodes.2nd_Territory, 'BLANK')
    where
      zipcodes.3DigitZip is not null
       and not zipcodes._DELETED
      and sales_org.territory not in ('BLANK'))
 """)
TMP_ORG2_IND_IND.count()
TMP_ORG2_IND_IND=TMP_ORG2_IND_IND.createOrReplaceTempView('TMP_ORG2_IND_IND')

# COMMAND ----------

TMP_ORG2_MED_HSS = spark.sql("""
select
  distinct
  0 createdBy,
  TO_TIMESTAMP(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS') AS createdOn,
  0 modifiedBy,
  TO_TIMESTAMP(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSS') AS modifiedOn,
  current_date insertedOn,
  current_date updatedOn,
  salesOrg salesOrganization,
  territory territoryID,
  division,
  zip3,
  vertical,
  salesRegion,
  userId,
  'ORG2' scenario
from
      (select
      sales_org.salesOrg,
      sales_org.territory,
      'MEDICAL' division,
      left(zipcodes.3DigitZip,3) zip3,
      sales_org.vertical,
      sales_org.region salesRegion,
      sales_org.user userId,
      sales_org.companyVisibility,
      'HSS' sourceFile
    from
      sales_org
      JOIN smartsheets.qvpos_hss_org_zip zipcodes on sales_org.territory = nvl(zipcodes.2nd_Territory, 'BLANK')
    where
      zipcodes.3DigitZip is not null
       and not zipcodes._DELETED
      and sales_org.territory not in ('BLANK'))
 """)
TMP_ORG2_MED_HSS.count()
TMP_ORG2_MED_HSS=TMP_ORG2_MED_HSS.createOrReplaceTempView('TMP_ORG2_MED_HSS')

# COMMAND ----------

sales_org2 = spark.sql("""
select * from TMP_ORG2_MED_ACUTE
UNION
select * from TMP_ORG2_IND_AUTO
UNION
select * from TMP_ORG2_MED_EMS
UNION
select * from TMP_ORG2_MED_DENTAL
UNION
select * from TMP_ORG2_IND_IND
UNION
select * from TMP_ORG2_MED_HSS
""")

# COMMAND ----------

# DUPLICATES
sales_org2_columns = ['vertical', 'zip3', 'scenario']

duplicates = get_duplicate_rows(sales_org2, sales_org2_columns)
duplicates.cache()

if not duplicates.rdd.isEmpty():
  notebook_data = {
    'source_name': 'CORE',
    'notebook_name': NOTEBOOK_NAME,
    'notebook_path': NOTEBOOK_PATH,
    'target_name': table_name,
    'duplicates_count': duplicates.count(),
    'duplicates_sample': duplicates.select(sales_org2_columns).limit(50)
  }
  send_mail_duplicate_records_found(notebook_data)

sales_org2_f = sales_org2.dropDuplicates(sales_org2_columns)
sales_org2_f.createOrReplaceTempView('sales_org2_f')
sales_org2_f.display()

# COMMAND ----------

main = spark.sql("""
  SELECT * FROM sales_org_default_f
  UNION
  SELECT * FROM sales_org2_f
""")

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

main = remove_duplicate_rows(df = main, 
                             key_columns = ['vertical', 'zip3','scenario'], 
                             tableName = table_name, 
                             sourceName = source_name, 
                             notebookName = NOTEBOOK_NAME, 
                             notebookPath = NOTEBOOK_PATH)

# COMMAND ----------

main_f = (
  main
  .transform(tg_default(source_name))
  .transform(tg_core_territory_assignments())
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
  
full_sales_org_default_keys = spark.sql("""
  SELECT
    DISTINCT
    zip3,
    vertical,
    scenario
  FROM sales_org_default_f
""")

full_sales_org2_keys = spark.sql("""
  SELECT
    DISTINCT
    zip3,
    vertical,
    scenario
  FROM sales_org2_f
""")

full_keys_f = (
  full_sales_org_default_keys
  .union(full_sales_org2_keys)
  .transform(attach_source_column(source = source_name))
  .transform(attach_surrogate_key(columns = 'vertical, zip3, scenario,_SOURCE'))
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

  cutoff_value = get_incr_col_max_value(qvpos_sales_reporting_org_structure)
  update_cutoff_value(cutoff_value, table_name, 'smartsheets.qvpos_sales_reporting_org_structure')
  update_run_datetime(run_datetime, table_name, 'smartsheets.qvpos_sales_reporting_org_structure')

  cutoff_value = get_incr_col_max_value(qvpos_sales_reporting_2nd_org_structure)
  update_cutoff_value(cutoff_value, table_name, 'smartsheets.qvpos_sales_reporting_2nd_org_structure')
  update_run_datetime(run_datetime, table_name, 'smartsheets.qvpos_sales_reporting_2nd_org_structure')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core
