# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.customer_location

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'ebs.hz_cust_acct_sites_all', prune_days)
  main_inc = load_incr_dataset('ebs.hz_cust_acct_sites_all', 'LAST_UPDATE_DATE', cutoff_value)
else:
  main_inc = load_full_dataset('ebs.hz_cust_acct_sites_all')
  

# COMMAND ----------

# SAMPLING
if sampling:
  main_inc = main_inc.limit(10)

# COMMAND ----------

# VIEWS
main_inc.createOrReplaceTempView('HZ_CUST_ACCT_SITES_ALL')

# COMMAND ----------

TMP_STATENAME_LKP = spark.sql("""select KEY_VALUE, CVALUE FROM smartsheets.edm_control_table where TABLE_ID = 'STATE_CODES'""")
TMP_STATENAME_LKP.createOrReplaceTempView('STATENAME_LKP')

# COMMAND ----------

main = spark.sql("""
  SELECT
    REPLACE(STRING(INT (HZ_LOCATIONS.CREATED_BY)), ",", "")  AS createdBy,
    TO_TIMESTAMP(HZ_LOCATIONS.CREATION_DATE,'yyyy-MM-dd HH:mm:ss.SSS') AS createdOn,
    REPLACE(STRING(INT (HZ_LOCATIONS.LAST_UPDATED_BY)), ",", "") AS modifiedBy,
    TO_TIMESTAMP(HZ_LOCATIONS.LAST_UPDATE_DATE, 'yyyy-MM-dd HH:mm:ss.SSS') AS modifiedOn,
    CURRENT_TIMESTAMP() AS insertedOn,
    CURRENT_TIMESTAMP() AS updatedOn, 
    REPLACE(STRING(INT (HZ_CUST_ACCT_SITES_ALL.CUST_ACCOUNT_ID)), ",", "") AS accountId,
    NULL AS accountNumber,
    HZ_CUST_SITE_USES_ALL.SITE_USE_CODE || '-'|| REPLACE(STRING(INT (HZ_CUST_SITE_USES_ALL.SITE_USE_ID)), ",", "") AS addressId,
    HZ_LOCATIONS.ADDRESS1 AS addressLine1,
    HZ_LOCATIONS.ADDRESS2 AS addressLine2,
    HZ_LOCATIONS.ADDRESS3 AS addressLine3,
    HZ_LOCATIONS.ADDRESS4 AS addressLine4,
    HZ_LOCATIONS.CITY AS city,
    HZ_LOCATIONS.COUNTRY AS country,
    HZ_LOCATIONS.COUNTRY AS countryCode,
    HZ_LOCATIONS.COUNTY as county,
    emailAddress.emailAddress AS emailAddress,
    HZ_CUST_ACCT_SITES_ALL.ATTRIBUTE16 AS indiaIrpCustomerType,
    HZ_PARTIES.ORIG_SYSTEM_REFERENCE AS mdmId,
    NULL AS name,
    REPLACE(STRING(INT (HZ_PARTY_SITES.PARTY_SITE_NUMBER)), ",", "") partySiteNumber,
    phoneNumber.phoneNumber AS phoneNumber,
    HZ_LOCATIONS.POSTAL_CODE AS postalCode,
    HZ_CUST_SITE_USES_ALL.PRIMARY_FLAG AS primaryFlag,
    NVL(HZ_CUST_ACCT_SITES_ALL.CUSTOMER_CATEGORY_CODE, 'NA') siteCategory,
    HZ_CUST_SITE_USES_ALL.SITE_USE_ID AS siteUseId,
    SITE_USE_CODE AS siteUseCode,
    STATENAME_LKP.CVALUE as stateName,
    HZ_LOCATIONS.STATE AS state
  FROM HZ_CUST_ACCT_SITES_ALL 
  INNER JOIN EBS.HZ_PARTY_SITES ON HZ_CUST_ACCT_SITES_ALL.PARTY_SITE_ID = HZ_PARTY_SITES.PARTY_SITE_ID
  INNER JOIN EBS.HZ_PARTIES ON HZ_PARTIES.PARTY_ID = HZ_PARTY_SITES.PARTY_ID
  INNER JOIN EBS.HZ_LOCATIONS ON HZ_PARTY_SITES.LOCATION_ID = HZ_LOCATIONS.LOCATION_ID
  INNER JOIN EBS.HZ_CUST_SITE_USES_ALL ON HZ_CUST_ACCT_SITES_ALL.CUST_ACCT_SITE_ID = HZ_CUST_SITE_USES_ALL.CUST_ACCT_SITE_ID
  LEFT JOIN  (SELECT 
                  owner_table_id, 
                  cp.email_address AS emailAddress, 
                  row_number() OVER (ORDER BY cp.email_address) AS rownum
              FROM ebs.hz_contact_points cp 
              WHERE 
                   cp.owner_table_name = 'HZ_PARTY_SITES' 
                   and cp.CONTACT_POINT_TYPE = 'EMAIL'
                   and cp.primary_flag = 'Y'
                   and cp.status = 'A'
                   ) As emailAddress
                   ON emailAddress.owner_table_id = HZ_PARTY_SITES.party_Site_id 
                   and emailAddress.rownum = 1
     LEFT JOIN  (SELECT 
                  owner_table_id, 
                  cp.RAW_PHONE_NUMBER AS phoneNumber, 
                  row_number() OVER (ORDER BY cp.RAW_PHONE_NUMBER) AS rownum
              FROM ebs.hz_contact_points cp 
              WHERE 
                   cp.owner_table_name = 'HZ_PARTY_SITES' 
                   and cp.CONTACT_POINT_TYPE = 'PHONE'
                   and cp.PHONE_LINE_TYPE = 'GEN'
                   and cp.status = 'A'
                   ) As phoneNumber
                   ON phoneNumber.owner_table_id = HZ_PARTY_SITES.party_Site_id 
                   and phoneNumber.rownum = 1
     LEFT JOIN STATENAME_LKP
       on STATENAME_LKP.KEY_VALUE = CONCAT(HZ_LOCATIONS.COUNTRY,'-',HZ_LOCATIONS.STATE )
  """)

display(main)

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

main = remove_duplicate_rows(df = main, 
                             key_columns = ['addressId'], 
                             tableName = table_name, 
                             sourceName = source_name, 
                             notebookName = NOTEBOOK_NAME, 
                             notebookPath = NOTEBOOK_PATH)

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .transform(tg_default(source_name))
  .transform(tg_core_customer_location())
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
full_keys = spark.sql("""
  SELECT
  HZ_CUST_SITE_USES_ALL.SITE_USE_CODE || '-'|| REPLACE(STRING(INT (HZ_CUST_SITE_USES_ALL.SITE_USE_ID)), ",", "") AS addressId
  FROM ebs.hz_cust_acct_sites_all HZ_CUST_ACCT_SITES_ALL
  INNER JOIN EBS.HZ_PARTY_SITES ON HZ_CUST_ACCT_SITES_ALL.PARTY_SITE_ID = HZ_PARTY_SITES.PARTY_SITE_ID
  INNER JOIN EBS.HZ_LOCATIONS  ON HZ_PARTY_SITES.LOCATION_ID = HZ_LOCATIONS.LOCATION_ID  
  INNER JOIN EBS.HZ_CUST_SITE_USES_ALL ON HZ_CUST_ACCT_SITES_ALL.CUST_ACCT_SITE_ID = HZ_CUST_SITE_USES_ALL.CUST_ACCT_SITE_ID
""")

full_keys_f = (
  full_keys
  .transform(attach_source_column(source = source_name))
  .transform(attach_primary_key('addressId,_SOURCE', 'edm.customer_location'))
  .select('_ID')
  .transform(add_unknown_ID())
)

apply_soft_delete(full_keys_f, table_name, key_columns = '_ID')

# COMMAND ----------

# UPDATE FK
update_foreign_key(table_name, 'createdBy,_SOURCE', 'createdBy_ID', 'edm.user')
update_foreign_key(table_name, 'modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user')
update_foreign_key(table_name, 'accountId,_SOURCE', 'account_ID', 'edm.account')

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not test_run:
  cutoff_value = get_incr_col_max_value(main_inc, 'LAST_UPDATE_DATE')
  update_cutoff_value(cutoff_value, table_name, 'EBS.HZ_CUST_ACCT_SITES_ALL')
  update_run_datetime(run_datetime, table_name, 'EBS.HZ_CUST_ACCT_SITES_ALL')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core
