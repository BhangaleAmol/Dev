# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ../_SCHEMA/g_comind.customer_extract

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_comind')
incremental = False
key_columns = get_input_param('key_columns', 'string', default_value = 'registration_id,cust_account_id,transaction_date')
overwrite = True
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'customer_extract')
target_folder = get_input_param('target_folder', default_value = '/datalake_gold/comind/full_data')

# COMMAND ----------

# VALIDATE INPUT
if incremental and key_columns is None:
  raise Exception("INCREMENTAL & NO KEY COLUMNS")

if overwrite and sampling:
  raise Exception("OVERWRITE & SAMPLING")

# COMMAND ----------

# VARIABLES
target_table = get_table_name(database_name, table_name)

# COMMAND ----------

# EXTRACT 
source_table = 'ebs.hz_cust_accounts'
if incremental:
  cutoff_value = get_cutoff_value(target_table, source_table, prune_days)  
  hz_cust_accounts = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  hz_cust_accounts = load_full_dataset(source_table)
  
hz_cust_accounts.createOrReplaceTempView('hz_cust_accounts')
hz_cust_accounts.display()

# COMMAND ----------

orgs=spark.sql("""
SELECT
  hao.organization_id,
  haotl.name
FROM
  ebs.hr_all_organization_units hao,
  ebs.hr_all_organization_units haotl
WHERE
  hao.organization_id = haotl.organization_id --AND haotl.language = 'US'
  """)
orgs .createOrReplaceTempView('orgs')

# COMMAND ----------

payment_terms=spark.sql("""
SELECT
  b.term_id,
  t.name,
  t.description
FROM
  ebs.ra_terms_tl t,
  ebs.ra_terms_b b
WHERE
  b.term_id = t.term_id
  AND t.language = 'US'
  """)
payment_terms .createOrReplaceTempView('payment_terms')

# COMMAND ----------

orgs=spark.sql("""
SELECT
  distinct hao.organization_id,
  haotl.name
FROM
  ebs.hr_all_organization_units hao,
  ebs.hr_all_organization_units haotl
WHERE
  hao.organization_id = haotl.organization_id -- AND haotl.language = 'US'
  """)
orgs.createOrReplaceTempView('orgs')

# COMMAND ----------

email=spark.sql("""
SELECT
  owner_table_id,
  max(hcp.email_address) email_address
FROM
  ebs.hz_contact_points hcp
WHERE
  contact_point_type = 'EMAIL' -- AND owner_table_id = hp1.party_id
  AND status = 'A' --AND ROWNUM = 1
group by
  owner_table_id
   """)
email.createOrReplaceTempView('email')

# COMMAND ----------

phone=spark.sql("""
SELECT
  owner_table_id,
  max(
    '+' || hcp.phone_country_code || ' ' || replace(hcp.raw_phone_number, '-', '')
  ) phone_number
FROM
  ebs.hz_contact_points hcp
WHERE
  contact_point_type = 'PHONE'
  --AND owner_table_id = hp1.party_id
  AND status = 'A' -- AND ROWNUM = 1
group by
  owner_table_id
  """)
phone .createOrReplaceTempView('phone')

# COMMAND ----------

ack_site_contacts=spark.sql("""
 SELECT
            account_number,
            party_id,
            CONCAT_WS(',',collect_list(contact_name)) contact_name,
            CONCAT_WS(',',collect_list(email_address)) email,
            CONCAT_WS(',',collect_list(phone_number)) phone_number,
            CONCAT_WS(',',collect_list(job_title)) job_title,
            CONCAT_WS(',',collect_list(address1)) address1,
            CONCAT_WS(',',collect_list(address2)) address2,
            CONCAT_WS(',',collect_list(address3)) address3,
            CONCAT_WS(',',collect_list(address4)) address4,
            CONCAT_WS(',',collect_list(city)) city,
            CONCAT_WS(',',collect_list(county)) county,
            CONCAT_WS(',',collect_list(state)) state,
              CONCAT_WS(',',collect_list(country)) country,
            CONCAT_WS(',',collect_list(postal_code)) postal_code
            
            
            FROM
            (

SELECT DISTINCT
                    hca.party_id,
                    hca.account_number,
                    substr(hp1.party_name, 1,(instr(hp1.party_name, hp.party_name) - 2)) contact_name,
                    email.email_address,
                    phone.phone_number,
                    hoc.job_title,
                    hp1.country,
                    hp1.address1,
                    hp1.address2,
                    hp1.address3,
                    hp1.address4,
                    hp1.city,
                    hp1.county,
                    hp1.state,
                    hp1.postal_code,
                    hz_role_responsibility.responsibility_type   contact_role,
                   date_format(hoc.creation_date, 'yyyyMMdd hh:mm:ss') hoc_creation_date,
                    date_format(hoc.last_update_date, 'yyyyMMdd hh:mm:ss') hoc_update_date,
                   date_format(hz_role_responsibility.creation_date, 'yyyyMMdd hh:mm:ss') role_creation_date,
                    date_format(hz_role_responsibility.last_update_date, 'yyyyMMdd hh:mm:ss') role_update_date,
                    ROW_NUMBER() OVER(
                        PARTITION BY hca.party_id
                        ORDER BY
                            hoc.creation_date, hoc.last_update_date, hz_role_responsibility.creation_date, hz_role_responsibility
                            .last_update_date
                    ) rn
                FROM
                    ebs.hz_cust_accounts hca,
                    ebs.hz_parties hp,
                    ebs.hz_parties hp1,
                    ebs.hz_cust_account_roles hcar,
                    ebs.hz_relationships hr,
                    ebs.hz_org_contacts hoc,
                    ebs.hz_role_responsibility,
                    email,
                    phone
                WHERE
                    1 = 1
                    AND hca.party_id = hp.party_id
                    AND hca.cust_account_id = hcar.cust_account_id
                    AND hcar.role_type = 'CONTACT'
                    AND hcar.party_id = hp1.party_id
                    AND hp1.party_type = 'PARTY_RELATIONSHIP'
                    AND hcar.status = 'A'
                    AND hcar.party_id = hr.party_id
                    AND hr.subject_id = hp.party_id
                    AND hr.relationship_id = hoc.party_relationship_id
                    AND hz_role_responsibility.cust_account_role_id = hcar.cust_account_role_id
                    AND hz_role_responsibility.responsibility_type = 'ACK'
                    AND hcar.cust_acct_site_id IS NOT NULL -- select only site level contacts
                    AND nvl(hoc.status, 'A') = 'A' -- select only active contacts
                    AND email.owner_table_id = hp1.party_id
                    AND phone.owner_table_id = hp1.party_id
                ORDER BY
                    hca.account_number,
                    hca.party_id,
                    hoc_creation_date,
                    hoc_update_date,
                    role_creation_date,
                    role_update_date
                     )
        WHERE
            rn <= 1  -- change here if we need to include more contact records     
        GROUP BY
            account_number,
            party_id
            """)
ack_site_contacts.createOrReplaceTempView('ack_site_contacts')
           

# COMMAND ----------

ack_account_contacts=spark.sql("""
 SELECT
            account_number,
            party_id,
            CONCAT_WS(',',collect_list(contact_name)) contact_name,
            CONCAT_WS(',',collect_list(email_address)) email,
            CONCAT_WS(',',collect_list(phone_number)) phone_number,
            CONCAT_WS(',',collect_list(job_title)) job_title,
            CONCAT_WS(',',collect_list(address1)) address1,
            CONCAT_WS(',',collect_list(address2)) address2,
            CONCAT_WS(',',collect_list(address3)) address3,
            CONCAT_WS(',',collect_list(address4)) address4,
            CONCAT_WS(',',collect_list(city)) city,
            CONCAT_WS(',',collect_list(county)) county,
            CONCAT_WS(',',collect_list(state)) state,
              CONCAT_WS(',',collect_list(country)) country,
            CONCAT_WS(',',collect_list(postal_code)) postal_code
            
            
            FROM
            (

SELECT DISTINCT
                    hca.party_id,
                    hca.account_number,
                    substr(hp1.party_name, 1,(instr(hp1.party_name, hp.party_name) - 2)) contact_name,
                    email.email_address,
                    phone.phone_number,
                    hoc.job_title,
                    hp1.country,
                    hp1.address1,
                    hp1.address2,
                    hp1.address3,
                    hp1.address4,
                    hp1.city,
                    hp1.county,
                    hp1.state,
                    hp1.postal_code,
                    hz_role_responsibility.responsibility_type   contact_role,
                   date_format(hoc.creation_date, 'yyyyMMdd hh:mm:ss') hoc_creation_date,
                    date_format(hoc.last_update_date, 'yyyyMMdd hh:mm:ss') hoc_update_date,
                   date_format(hz_role_responsibility.creation_date, 'yyyyMMdd hh:mm:ss') role_creation_date,
                    date_format(hz_role_responsibility.last_update_date, 'yyyyMMdd hh:mm:ss') role_update_date,
                    ROW_NUMBER() OVER(
                        PARTITION BY hca.party_id
                        ORDER BY
                            hoc.creation_date, hoc.last_update_date, hz_role_responsibility.creation_date, hz_role_responsibility
                            .last_update_date
                    ) rn
                FROM
                    ebs.hz_cust_accounts hca,
                    ebs.hz_parties hp,
                    ebs.hz_parties hp1,
                    ebs.hz_cust_account_roles hcar,
                    ebs.hz_relationships hr,
                    ebs.hz_org_contacts hoc,
                    ebs.hz_role_responsibility,
                    email,
                    phone
                WHERE
                    1 = 1
                    AND hca.party_id = hp.party_id
                    AND hca.cust_account_id = hcar.cust_account_id
                    AND hcar.role_type = 'CONTACT'
                    AND hcar.party_id = hp1.party_id
                    AND hp1.party_type = 'PARTY_RELATIONSHIP'
                    AND hcar.status = 'A'
                    AND hcar.party_id = hr.party_id
                    AND hr.subject_id = hp.party_id
                    AND hr.relationship_id = hoc.party_relationship_id
                    AND hz_role_responsibility.cust_account_role_id = hcar.cust_account_role_id
                    AND hz_role_responsibility.responsibility_type = 'ACK'
                    AND hcar.cust_acct_site_id IS NULL -- select only site level contacts
                    AND nvl(hoc.status, 'A') = 'A' -- select only active contacts
                    AND email.owner_table_id = hp1.party_id
                    AND phone.owner_table_id = hp1.party_id
                ORDER BY
                    hca.account_number,
                    hca.party_id,
                    hoc_creation_date,
                    hoc_update_date,
                    role_creation_date,
                    role_update_date
                     )
        WHERE
            rn <= 1  -- change here if we need to include more contact records     
        GROUP BY
            account_number,
            party_id
            """)
ack_account_contacts.createOrReplaceTempView('ack_account_contacts')
           

# COMMAND ----------

inv_site_contacts=spark.sql("""
 SELECT
            account_number,
            party_id,
            CONCAT_WS(',',collect_list(contact_name)) contact_name,
            CONCAT_WS(',',collect_list(email_address)) email,
            CONCAT_WS(',',collect_list(phone_number)) phone_number,
            CONCAT_WS(',',collect_list(job_title)) job_title,
            CONCAT_WS(',',collect_list(address1)) address1,
            CONCAT_WS(',',collect_list(address2)) address2,
            CONCAT_WS(',',collect_list(address3)) address3,
            CONCAT_WS(',',collect_list(address4)) address4,
            CONCAT_WS(',',collect_list(city)) city,
            CONCAT_WS(',',collect_list(county)) county,
            CONCAT_WS(',',collect_list(state)) state,
              CONCAT_WS(',',collect_list(country)) country,
            CONCAT_WS(',',collect_list(postal_code)) postal_code
            
            
            FROM
            (

SELECT DISTINCT
                    hca.party_id,
                    hca.account_number,
                    substr(hp1.party_name, 1,(instr(hp1.party_name, hp.party_name) - 2)) contact_name,
                    email.email_address,
                    phone.phone_number,
                    hoc.job_title,
                    hp1.country,
                    hp1.address1,
                    hp1.address2,
                    hp1.address3,
                    hp1.address4,
                    hp1.city,
                    hp1.county,
                    hp1.state,
                    hp1.postal_code,
                    hz_role_responsibility.responsibility_type   contact_role,
                   date_format(hoc.creation_date, 'yyyyMMdd hh:mm:ss') hoc_creation_date,
                    date_format(hoc.last_update_date, 'yyyyMMdd hh:mm:ss') hoc_update_date,
                   date_format(hz_role_responsibility.creation_date, 'yyyyMMdd hh:mm:ss') role_creation_date,
                    date_format(hz_role_responsibility.last_update_date, 'yyyyMMdd hh:mm:ss') role_update_date,
                    ROW_NUMBER() OVER(
                        PARTITION BY hca.party_id
                        ORDER BY
                            hoc.creation_date, hoc.last_update_date, hz_role_responsibility.creation_date, hz_role_responsibility
                            .last_update_date
                    ) rn
                FROM
                    ebs.hz_cust_accounts hca,
                    ebs.hz_parties hp,
                    ebs.hz_parties hp1,
                    ebs.hz_cust_account_roles hcar,
                    ebs.hz_relationships hr,
                    ebs.hz_org_contacts hoc,
                    ebs.hz_role_responsibility,
                    email,
                    phone
                WHERE
                    1 = 1
                    AND hca.party_id = hp.party_id
                    AND hca.cust_account_id = hcar.cust_account_id
                    AND hcar.role_type = 'CONTACT'
                    AND hcar.party_id = hp1.party_id
                    AND hp1.party_type = 'PARTY_RELATIONSHIP'
                    AND hcar.status = 'A'
                    AND hcar.party_id = hr.party_id
                    AND hr.subject_id = hp.party_id
                    AND hr.relationship_id = hoc.party_relationship_id
                    AND hz_role_responsibility.cust_account_role_id = hcar.cust_account_role_id
                    AND hz_role_responsibility.responsibility_type = 'INV'
                    AND hcar.cust_acct_site_id IS NOT NULL -- select only site level contacts
                    AND nvl(hoc.status, 'A') = 'A' -- select only active contacts
                    AND email.owner_table_id = hp1.party_id
                    AND phone.owner_table_id = hp1.party_id
                ORDER BY
                    hca.account_number,
                    hca.party_id,
                    hoc_creation_date,
                    hoc_update_date,
                    role_creation_date,
                    role_update_date
                     )
        WHERE
            rn <= 1  -- change here if we need to include more contact records     
        GROUP BY
            account_number,
            party_id
            """)
inv_site_contacts.createOrReplaceTempView('inv_site_contacts')
           

# COMMAND ----------

inv_site_contacts=spark.sql("""
 SELECT
            account_number,
            party_id,
            CONCAT_WS(',',collect_list(contact_name)) contact_name,
            CONCAT_WS(',',collect_list(email_address)) email,
            CONCAT_WS(',',collect_list(phone_number)) phone_number,
            CONCAT_WS(',',collect_list(job_title)) job_title,
            CONCAT_WS(',',collect_list(address1)) address1,
            CONCAT_WS(',',collect_list(address2)) address2,
            CONCAT_WS(',',collect_list(address3)) address3,
            CONCAT_WS(',',collect_list(address4)) address4,
            CONCAT_WS(',',collect_list(city)) city,
            CONCAT_WS(',',collect_list(county)) county,
            CONCAT_WS(',',collect_list(state)) state,
              CONCAT_WS(',',collect_list(country)) country,
            CONCAT_WS(',',collect_list(postal_code)) postal_code
            
            
            FROM
            (

SELECT DISTINCT
                    hca.party_id,
                    hca.account_number,
                    substr(hp1.party_name, 1,(instr(hp1.party_name, hp.party_name) - 2)) contact_name,
                    email.email_address,
                    phone.phone_number,
                    hoc.job_title,
                    hp1.country,
                    hp1.address1,
                    hp1.address2,
                    hp1.address3,
                    hp1.address4,
                    hp1.city,
                    hp1.county,
                    hp1.state,
                    hp1.postal_code,
                    hz_role_responsibility.responsibility_type   contact_role,
                   date_format(hoc.creation_date, 'yyyyMMdd hh:mm:ss') hoc_creation_date,
                    date_format(hoc.last_update_date, 'yyyyMMdd hh:mm:ss') hoc_update_date,
                   date_format(hz_role_responsibility.creation_date, 'yyyyMMdd hh:mm:ss') role_creation_date,
                    date_format(hz_role_responsibility.last_update_date, 'yyyyMMdd hh:mm:ss') role_update_date,
                    ROW_NUMBER() OVER(
                        PARTITION BY hca.party_id
                        ORDER BY
                            hoc.creation_date, hoc.last_update_date, hz_role_responsibility.creation_date, hz_role_responsibility
                            .last_update_date
                    ) rn
                FROM
                    ebs.hz_cust_accounts hca,
                    ebs.hz_parties hp,
                    ebs.hz_parties hp1,
                    ebs.hz_cust_account_roles hcar,
                    ebs.hz_relationships hr,
                    ebs.hz_org_contacts hoc,
                    ebs.hz_role_responsibility,
                    email,
                    phone
                WHERE
                    1 = 1
                    AND hca.party_id = hp.party_id
                    AND hca.cust_account_id = hcar.cust_account_id
                    AND hcar.role_type = 'CONTACT'
                    AND hcar.party_id = hp1.party_id
                    AND hp1.party_type = 'PARTY_RELATIONSHIP'
                    AND hcar.status = 'A'
                    AND hcar.party_id = hr.party_id
                    AND hr.subject_id = hp.party_id
                    AND hr.relationship_id = hoc.party_relationship_id
                    AND hz_role_responsibility.cust_account_role_id = hcar.cust_account_role_id
                    AND hz_role_responsibility.responsibility_type = 'INV'
                    AND hcar.cust_acct_site_id IS NOT NULL -- select only site level contacts
                    AND nvl(hoc.status, 'A') = 'A' -- select only active contacts
                    AND email.owner_table_id = hp1.party_id
                    AND phone.owner_table_id = hp1.party_id
                ORDER BY
                    hca.account_number,
                    hca.party_id,
                    hoc_creation_date,
                    hoc_update_date,
                    role_creation_date,
                    role_update_date
                     )
        WHERE
            rn <= 1  -- change here if we need to include more contact records     
        GROUP BY
            account_number,
            party_id
            """)
inv_site_contacts.createOrReplaceTempView('inv_site_contacts')
           

# COMMAND ----------

inv_account_contacts=spark.sql("""
 SELECT
            account_number,
            party_id,
            CONCAT_WS(',',collect_list(contact_name)) contact_name,
            CONCAT_WS(',',collect_list(email_address)) email,
            CONCAT_WS(',',collect_list(phone_number)) phone_number,
            CONCAT_WS(',',collect_list(job_title)) job_title,
            CONCAT_WS(',',collect_list(address1)) address1,
            CONCAT_WS(',',collect_list(address2)) address2,
            CONCAT_WS(',',collect_list(address3)) address3,
            CONCAT_WS(',',collect_list(address4)) address4,
            CONCAT_WS(',',collect_list(city)) city,
            CONCAT_WS(',',collect_list(county)) county,
            CONCAT_WS(',',collect_list(state)) state,
              CONCAT_WS(',',collect_list(country)) country,
            CONCAT_WS(',',collect_list(postal_code)) postal_code
            
            
            FROM
            (

SELECT DISTINCT
                    hca.party_id,
                    hca.account_number,
                    substr(hp1.party_name, 1,(instr(hp1.party_name, hp.party_name) - 2)) contact_name,
                    email.email_address,
                    phone.phone_number,
                    hoc.job_title,
                    hp1.country,
                    hp1.address1,
                    hp1.address2,
                    hp1.address3,
                    hp1.address4,
                    hp1.city,
                    hp1.county,
                    hp1.state,
                    hp1.postal_code,
                    hz_role_responsibility.responsibility_type   contact_role,
                   date_format(hoc.creation_date, 'yyyyMMdd hh:mm:ss') hoc_creation_date,
                    date_format(hoc.last_update_date, 'yyyyMMdd hh:mm:ss') hoc_update_date,
                   date_format(hz_role_responsibility.creation_date, 'yyyyMMdd hh:mm:ss') role_creation_date,
                    date_format(hz_role_responsibility.last_update_date, 'yyyyMMdd hh:mm:ss') role_update_date,
                    ROW_NUMBER() OVER(
                        PARTITION BY hca.party_id
                        ORDER BY
                            hoc.creation_date, hoc.last_update_date, hz_role_responsibility.creation_date, hz_role_responsibility
                            .last_update_date
                    ) rn
                FROM
                    ebs.hz_cust_accounts hca,
                    ebs.hz_parties hp,
                    ebs.hz_parties hp1,
                    ebs.hz_cust_account_roles hcar,
                    ebs.hz_relationships hr,
                    ebs.hz_org_contacts hoc,
                    ebs.hz_role_responsibility,
                    email,
                    phone
                WHERE
                    1 = 1
                    AND hca.party_id = hp.party_id
                    AND hca.cust_account_id = hcar.cust_account_id
                    AND hcar.role_type = 'CONTACT'
                    AND hcar.party_id = hp1.party_id
                    AND hp1.party_type = 'PARTY_RELATIONSHIP'
                    AND hcar.status = 'A'
                    AND hcar.party_id = hr.party_id
                    AND hr.subject_id = hp.party_id
                    AND hr.relationship_id = hoc.party_relationship_id
                    AND hz_role_responsibility.cust_account_role_id = hcar.cust_account_role_id
                    AND hz_role_responsibility.responsibility_type = 'INV'
                    AND hcar.cust_acct_site_id IS NULL -- select only site level contacts
                    AND nvl(hoc.status, 'A') = 'A' -- select only active contacts
                    AND email.owner_table_id = hp1.party_id
                    AND phone.owner_table_id = hp1.party_id
                ORDER BY
                    hca.account_number,
                    hca.party_id,
                    hoc_creation_date,
                    hoc_update_date,
                    role_creation_date,
                    role_update_date
                     )
        WHERE
            rn <= 1  -- change here if we need to include more contact records     
        GROUP BY
            account_number,
            party_id
            """)
inv_account_contacts.createOrReplaceTempView('inv_account_contacts')
           

# COMMAND ----------

main_customer_extract = spark.sql("""
SELECT
  *
FROM
  (
    SELECT
      DISTINCT hz_parties.party_number registration_id,
      REPLACE(STRING(INT (hz_cust_accounts.cust_account_id)), ",", "")  cust_account_id,
      REPLACE(STRING(INT (hz_customer_profiles.cust_account_profile_id)), ",", "")  cust_account_profile_id,
      REPLACE(STRING(INT (hz_cust_acct_sites_all.cust_acct_site_id)), ",", "")  cust_acct_site_id,
      REPLACE(STRING(INT (hz_cust_accounts.party_id)), ",", "") party_id,
      hz_cust_accounts.account_number,
      party_sites.party_site_number site_use_id,
      '' party_tax_profile_id,
      orgs.name operating_unit,
      hz_cust_accounts.account_name organization_name,
      hz_cust_accounts.account_name account_description,
      CASE
        WHEN hz_cust_accounts.customer_type = 'R' THEN 'External'
        WHEN hz_cust_accounts.customer_type = 'I' THEN 'Internal'
      END internal_external_account,
      nvl(hz_cust_accounts.attribute1, '') gbu,
      TRIM(hz_cust_accounts.attribute2) || '-' || TRIM(hz_cust_accounts.attribute3) industry_subindustry,
      nvl(hz_cust_accounts.attribute4, '') account_type,
      nvl(hz_cust_accounts.attribute11, '') customer_vertical,
      nvl(hz_cust_accounts.attribute12, '') customer_type,
      nvl(hz_cust_accounts.attribute13, '') customer_division,
      CASE
        WHEN nvl(
          hz_cust_acct_sites_all.customer_category_code,
          'Undefined'
        ) = 'LA' THEN 'Latin America'
        ELSE ''
      END category,
      'G001' shipping_case_label_format,
      'P001' pallet_label_format,
      'G00' pallet_bar_coding_symbology,
      hz_cust_acct_sites_all.attribute13 special_freight_terms,
      CASE
        WHEN nvl(hz_cust_site_uses_all.site_use_code, 'N') = 'BILL_TO' THEN 'YES'
        ELSE 'NO'
      END bill_to,
      CASE
        WHEN nvl(hz_cust_site_uses_all.site_use_code, 'N') = 'SOLD_TO' THEN 'YES'
        ELSE 'NO'
      END sold_to,
      CASE
        WHEN nvl(hz_cust_site_uses_all.site_use_code, 'N') = 'SHIP_TO' THEN 'YES'
        ELSE 'NO'
      END ship_to,
      'NO' weekly_shipment,
      CASE
        WHEN nvl(hz_cust_site_uses_all.site_use_code, 'N') = 'BILL_TO' THEN bill_to_terr.territory_short_name
      END bill_to_country,
      CASE
        WHEN nvl(hz_cust_site_uses_all.site_use_code, 'N') = 'BILL_TO' THEN hz_locations.address1
      END bill_to_address1,
      CASE
        WHEN nvl(hz_cust_site_uses_all.site_use_code, 'N') = 'BILL_TO' THEN hz_locations.address2
      END bill_to_address2,
      CASE
        WHEN nvl(hz_cust_site_uses_all.site_use_code, 'N') = 'BILL_TO' THEN hz_locations.address3
      END bill_to_address3,
      CASE
        WHEN nvl(hz_cust_site_uses_all.site_use_code, 'N') = 'BILL_TO' THEN hz_locations.address4
      END bill_to_address4,
      CASE
        WHEN nvl(hz_cust_site_uses_all.site_use_code, 'N') = 'BILL_TO' THEN hz_locations.city
      END bill_to_city,
      CASE
        WHEN nvl(hz_cust_site_uses_all.site_use_code, 'N') = 'BILL_TO' THEN hz_locations.county
      END bill_to_county,
      CASE
        WHEN nvl(hz_cust_site_uses_all.site_use_code, 'N') = 'BILL_TO' THEN hz_locations.state
      END bill_to_state,
      CASE
        WHEN nvl(hz_cust_site_uses_all.site_use_code, 'N') = 'BILL_TO' THEN hz_locations.postal_code
      END bill_to_postal_code,
      ar_collectors.name collector,
      CASE
        WHEN hz_customer_profiles.credit_checking = 'Y' THEN 'YES'
        WHEN hz_customer_profiles.credit_checking = 'N' THEN 'NO'
      END credit_check,
      CASE
        WHEN hz_customer_profiles.credit_hold = 'Y' THEN 'YES'
        WHEN hz_customer_profiles.credit_hold = 'N' THEN 'NO'
      END credit_hold,
      payment_terms.name payment_terms,
      CASE
        WHEN nvl(
          hz_customer_profiles.discount_terms,
          nvl(hz_cust_profile_classes.discount_terms, 'N')
        ) = 'Y' THEN 'YES'
        WHEN nvl(
          hz_customer_profiles.discount_terms,
          nvl(hz_cust_profile_classes.discount_terms, 'N')
        ) = 'N' THEN 'NO'
      END allow_discount,
      nvl(
        hz_customer_profiles.discount_grace_days,
        hz_cust_profile_classes.discount_grace_days
      ) discount_grace_days,
      CASE
        WHEN nvl(
          hz_customer_profiles.dunning_letters,
          nvl(hz_cust_profile_classes.dunning_letters, 'N')
        ) = 'Y' THEN 'YES'
        WHEN nvl(
          hz_customer_profiles.dunning_letters,
          nvl(hz_cust_profile_classes.dunning_letters, 'N')
        ) = 'N' THEN 'NO'
      END dunning_letters,
      (
        CASE
          WHEN hz_cust_accounts.customer_type = 'R' THEN gl_code_combinations.segment1 || '.' || gl_code_combinations.segment2 || '.' || gl_code_combinations.segment3 || '.' || gl_code_combinations.segment4 || '.' || gl_code_combinations.segment5 || '.' || gl_code_combinations.segment6 || '.' || gl_code_combinations.segment7
        END
      ) receivables_account,
      (
        CASE
          WHEN hz_cust_accounts.customer_type = 'R' THEN gl_code_combinations1.segment1 || '.' || gl_code_combinations1.segment2 || '.' || gl_code_combinations1.segment3 || '.' || gl_code_combinations1.segment4 || '.' || gl_code_combinations1.segment5 || '.' || gl_code_combinations1.segment6 || '.' || gl_code_combinations1.segment7
        END
      ) revenue_account,
      (
        CASE
          WHEN hz_cust_accounts.customer_type = 'R' THEN gl_code_combinations2.segment1 || '.' || gl_code_combinations2.segment2 || '.' || gl_code_combinations2.segment3 || '.' || gl_code_combinations2.segment4 || '.' || gl_code_combinations2.segment5 || '.' || gl_code_combinations2.segment6 || '.' || gl_code_combinations2.segment7
        END
      ) tax_account,
      (
        CASE
          WHEN hz_cust_accounts.customer_type = 'R' THEN gl_code_combinations3.segment1 || '.' || gl_code_combinations3.segment2 || '.' || gl_code_combinations3.segment3 || '.' || gl_code_combinations3.segment4 || '.' || gl_code_combinations3.segment5 || '.' || gl_code_combinations3.segment6 || '.' || gl_code_combinations3.segment7
        END
      ) freight_account,
      nvl(
        hz_cust_site_uses_all.freight_term,
        hz_cust_accounts.freight_term
      ) freight_terms,
      hr_all_organization_units1.name warehouse,
      '' certificate_number,
      '' exempt_reason,
      '' exempt_status,
      '' tax_regime_code,
      '' configuration_owner,
      '' exemption_rate_type,
      '' discount_percentage,
      '' effective_from,
      '' default_reporting_registration,
      CASE
        WHEN hz_cust_accounts.status = 'I' THEN 'Inactive'
        ELSE 'Active'
      END active_flg,
      hz_cust_accounts.ATTRIBUTE19 customer_tier,
      hz_cust_accounts.ATTRIBUTE20 customer_segmentation,
      nvl(
        hz_cust_site_uses_all.FOB_POINT,
        hz_cust_accounts.FOB_POINT
      ) fob_point,
      ack_account_contacts.contact_name ack_account_contact_name,
      CASE
        WHEN nvl(hz_cust_site_uses_all.site_use_code, 'N') = 'BILL_TO' THEN hz_cust_site_uses_all.ATTRIBUTE10
      END bill_to_subregion,
      hz_cust_acct_sites_all.ATTRIBUTE16 india_irp_customer_type,
      hz_cust_accounts.ATTRIBUTE8 customer_forecast_group,
      hz_cust_accounts.ATTRIBUTE18 web_store_enabled,
      current_date() transaction_date
    FROM
      ebs.hz_cust_accounts hz_cust_accounts
      JOIN ebs.hz_parties ON hz_cust_accounts.party_id = hz_parties.party_id
      JOIN ebs.hz_party_sites party_sites ON hz_parties.party_id = party_sites.party_id
      JOIN ebs.hz_cust_acct_sites_all hz_cust_acct_sites_all ON hz_cust_accounts.cust_account_id = hz_cust_acct_sites_all.cust_account_id
      AND party_sites.party_site_id = hz_cust_acct_sites_all.party_site_id
      JOIN ebs.hz_locations ON party_sites.location_id = hz_locations.location_id
      JOIN ebs.hz_cust_site_uses_all hz_cust_site_uses_all ON hz_cust_acct_sites_all.cust_acct_site_id = hz_cust_site_uses_all.cust_acct_site_id
      LEFT JOIN orgs ON hz_cust_acct_sites_all.org_id = orgs.organization_id
      JOIN ebs.fnd_territories_tl bill_to_terr ON hz_locations.country = bill_to_terr.territory_code
      and bill_to_terr.language = 'US'
      LEFT JOIN ebs.hz_customer_profiles ON hz_cust_accounts.cust_account_id = hz_customer_profiles.cust_account_id
      AND hz_cust_site_uses_all.site_use_id = hz_customer_profiles.site_use_id
      LEFT JOIN ebs.hz_cust_profile_classes ON hz_customer_profiles.profile_class_id = hz_cust_profile_classes.profile_class_id
      LEFT JOIN ebs.ar_collectors ON hz_customer_profiles.collector_id = ar_collectors.collector_id
      LEFT JOIN ebs.hz_cust_profile_amts ON hz_customer_profiles.cust_account_id = hz_cust_profile_amts.cust_account_id
      LEFT JOIN payment_terms ON nvl(
        hz_cust_site_uses_all.payment_term_id,
        hz_cust_accounts.payment_term_id
      ) = payment_terms.term_id
      LEFT JOIN ebs.gl_code_combinations ON gl_code_combinations.code_combination_id = hz_cust_site_uses_all.gl_id_rec
      LEFT JOIN ebs.gl_code_combinations gl_code_combinations1 ON gl_code_combinations1.code_combination_id = hz_cust_site_uses_all.gl_id_rev
      LEFT JOIN ebs.gl_code_combinations gl_code_combinations2 ON gl_code_combinations2.code_combination_id = hz_cust_site_uses_all.gl_id_tax
      LEFT JOIN ebs.gl_code_combinations gl_code_combinations3 ON gl_code_combinations3.code_combination_id = hz_cust_site_uses_all.gl_id_freight
      LEFT JOIN ebs.hr_all_organization_units hr_all_organization_units1 ON hr_all_organization_units1.organization_id = hz_cust_site_uses_all.warehouse_id
      LEFT JOIN ack_site_contacts ON hz_cust_accounts.party_id = ack_site_contacts.party_id
      AND hz_cust_accounts.account_number = ack_site_contacts.account_number
      LEFT JOIN ack_account_contacts ON hz_cust_accounts.party_id = ack_account_contacts.party_id
      AND hz_cust_accounts.account_number = ack_account_contacts.account_number
      LEFT JOIN inv_site_contacts ON hz_cust_accounts.party_id = inv_site_contacts.party_id
      AND hz_cust_accounts.account_number = inv_site_contacts.account_number
      LEFT JOIN inv_account_contacts ON hz_cust_accounts.party_id = inv_account_contacts.party_id
      AND hz_cust_accounts.account_number = inv_account_contacts.account_number
    WHERE
      orgs.name NOT IN (
        'Ansell Hawkeye Inc',
        'Ansell Protective Products Inc',
        'SXWELL USA LLC'
      )
 
      AND (
        nvl(hz_cust_accounts.status, 'A') = 'A'
        OR (
          date_format(hz_cust_accounts.last_update_date, 'yyyy') >= '2018'
          AND hz_cust_accounts.status = 'I'
        )
      )
      AND nvl(hz_cust_acct_sites_all.status, 'A') = 'A'
      AND nvl(hz_cust_site_uses_all.site_use_code, 'N') = 'BILL_TO'
  )
WHERE
  BILL_TO = 'YES'
   """)
main_customer_extract.createOrReplaceTempView("main_customer_extract")

# COMMAND ----------

# TRANSFORM DATA
main_f = (
   main_customer_extract
   .transform(attach_partition_column("transaction_date"))
   .transform(attach_modified_date())
   .transform(attach_deleted_flag())
   .transform(apply_schema(schema))
)

if incremental:
  main_f.display()

# COMMAND ----------

# LOAD
options = {'overwrite': overwrite, 'partition_column': '_PART'}
register_hive_table(main_f, target_table, target_folder, options = options)
merge_into_table(main_f, target_table, key_columns, options = {'auto_merge': True})
