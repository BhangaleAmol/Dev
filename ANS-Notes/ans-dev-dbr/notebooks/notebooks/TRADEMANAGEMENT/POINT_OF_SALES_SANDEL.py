# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_trademanagement

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_trademanagement.point_of_sales_sandel

# COMMAND ----------

# PARAMETERS
incremental = False
overwrite = True
sampling = False
source_name = "EBS"

# COMMAND ----------

# EXTRACT
file_name = 's_trademanagement.clean_territory_assignments.dlt'
file_path = get_file_path(temp_folder, file_name, target_container, target_storage)
terr_df = spark.read.format('delta').load(file_path)

sandel = spark.table('pam.dbo_vw_sandel_total')

# COMMAND ----------

# VIEWS
sandel.createOrReplaceTempView('sandel')
terr_df.createOrReplaceTempView('terr_df')

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

# COMMAND ----------

ZIP3=spark.sql("""
select
  min(territoryid) territoryid,
  trim(zip3) zip3,
  vertical,
  min(salesregion) sales_region,
  min(userid) user_id
from
 terr_df
where
  zip3 is not null
  and TerritoryID not like '%HSE'
  and scenario = 'DEFAULT'
group by
  trim(zip3),
  vertical
""")

ZIP3.createOrReplaceTempView('ZIP3')

# COMMAND ----------

ZIP0=spark.sql("""
select
  territoryid,
  vertical,
  salesregion,
  min(userid) user_id
from
  terr_df
where
  zip3 is null
  and TerritoryID not like '%HSE'
  and scenario = 'DEFAULT'
group by
  territoryid,
  vertical,
  salesregion
""")

ZIP0.createOrReplaceTempView('ZIP0')

# COMMAND ----------

TERR_DEFAULT=spark.sql("""
select
  *
from
  terr_df
where
  scenario = 'DEFAULT'
""")

TERR_DEFAULT.createOrReplaceTempView('TERR_DEFAULT')

# COMMAND ----------

SECONDORG_ZIP3=spark.sql("""
select
  min(territoryid) territoryid,
  trim(zip3) zip3,
  vertical,
  min(salesregion) sales_region,
  min(userid) user_id
from
  terr_df
where
  zip3 is not null
  and TerritoryID not like '%HSE'
  and scenario = 'ORG2'
group by
  trim(zip3),
  vertical
""")

SECONDORG_ZIP3.createOrReplaceTempView('SECONDORG_ZIP3')

# COMMAND ----------

THIRDORG_ZIP3=spark.sql("""
select
  min(territoryid) territoryid,
  trim(zip3) zip3,
  vertical,
  min(salesregion) sales_region,
  min(userid) user_id
from
  terr_df
where
  zip3 is not null
  and TerritoryID not like '%HSE'
  and scenario = 'ORG3'
group by
  trim(zip3),
  vertical
""")

THIRDORG_ZIP3.createOrReplaceTempView('THIRDORG_ZIP3')

# COMMAND ----------

CUST_OVERRIDE=spark.sql("""
SELECT
  DISTINCT CustomerID CUSTID,
  MAPTOTERRITORY
FROM
  smartsheets.qvpos_customer_territory_override
WHERE
  MaptoTerritory IS NOT NULL
""")

CUST_OVERRIDE.createOrReplaceTempView('CUST_OVERRIDE')

# COMMAND ----------

SECONDORG_CUST_OVERRIDE=spark.sql("""
SELECT
  DISTINCT CustomerID CUSTID,
  MAPTOTERRITORY
FROM
  smartsheets.qvpos_customer_territory_override
WHERE
  MapTo2ndOrgTerritory IS NOT NULL
""")

SECONDORG_CUST_OVERRIDE.createOrReplaceTempView('SECONDORG_CUST_OVERRIDE')

# COMMAND ----------

THIRDORG_CUST_OVERRIDE=spark.sql("""
SELECT
  DISTINCT CustomerID CUSTID,
  MAPTOTERRITORY
FROM
  smartsheets.qvpos_customer_territory_override
WHERE
  MapToOneS_Territory IS NOT NULL
""")

THIRDORG_CUST_OVERRIDE.createOrReplaceTempView('THIRDORG_CUST_OVERRIDE')

# COMMAND ----------

PROD_OVERRIDE=spark.sql("""
SELECT
  *
FROM
  smartsheets.qvpos_product_territory_override
WHERE
  MaptoTerritory IS NOT NULL
""")

PROD_OVERRIDE.createOrReplaceTempView('PROD_OVERRIDE')

# COMMAND ----------

SECONDORG_PROD_OVERRIDE=spark.sql("""
SELECT
  *
FROM
  smartsheets.qvpos_product_territory_override
WHERE
  MapTo2ndOrgTerritory IS NOT NULL
""")

SECONDORG_PROD_OVERRIDE.createOrReplaceTempView('SECONDORG_PROD_OVERRIDE')

# COMMAND ----------

THIRDORG_PROD_OVERRIDE=spark.sql("""
SELECT
  *
FROM
  smartsheets.qvpos_product_territory_override
WHERE
  MapToOneS_Territory IS NOT NULL
""")

THIRDORG_PROD_OVERRIDE.createOrReplaceTempView('THIRDORG_PROD_OVERRIDE')

# COMMAND ----------

main_stage = spark.sql("""
SELECT
COMPANY,
DIVISION,
SALESREGION,
SUBREGION,
TERRITORY,
VERTICAL,
CUSTOMERTYPE,
DISTRIBUTORID,
DISTRIBUTORNAME,
CITY,
STATE,
POSTALCODE,
ENDUSER,
GBU,
PRODCATEGORY,
PRODSUBCATEGORY,
MATERIALTYPE,
MATERIALSUBTYPE,
BRANDTYPE,
BRANDOWNER,
BRAND,
INDUSTRY,
PRODUCTSTYLE,
FAMILYID,
FAMILYDESCR,
SKUID,
SKUDESCR,
TRANDATE,
SUM(QTY) QTY,
EQUIVUNIT,
SUM(TRANAMT) TRANAMT,
USERID,
ID,
LASTMODIFIEDDATE,
ONESOLUTION_TERRITORY,
ONESOLUTION_SALESREGION,
ONESOLUTION_USERID,
_DELETED,
_MODIFIED
FROM
SANDEL
GROUP BY
COMPANY,
DIVISION,
SALESREGION,
SUBREGION,
TERRITORY,
VERTICAL,
CUSTOMERTYPE,
DISTRIBUTORID,
DISTRIBUTORNAME,
CITY,
STATE,
POSTALCODE,
ENDUSER,
GBU,
PRODCATEGORY,
PRODSUBCATEGORY,
MATERIALTYPE,
MATERIALSUBTYPE,
BRANDTYPE,
BRANDOWNER,
BRAND,
INDUSTRY,
PRODUCTSTYLE,
FAMILYID,
FAMILYDESCR,
SKUID,
SKUDESCR,
TRANDATE,
EQUIVUNIT,
USERID,
ID,
LASTMODIFIEDDATE,
ONESOLUTION_TERRITORY,
ONESOLUTION_SALESREGION,
ONESOLUTION_USERID,
_DELETED,
_MODIFIED
""")

main_stage.createOrReplaceTempView('main_stage')

# COMMAND ----------

main = spark.sql("""
SELECT
  cast(NULL as string) AS createdBy,
  sandel.Trandate AS createdOn,
  cast(NULL AS STRING) AS modifiedBy,
  cast(NULL AS timestamp) AS modifiedOn,
  CURRENT_TIMESTAMP() AS insertedOn,
  CURRENT_TIMESTAMP() AS updatedOn,
  sandel.brand,
  sandel.brandowner,
  sandel.brandtype,
  sandel.city,
  sandel.company,
  sandel.customertype,
  sandel.distributorid,
  sandel.distributorname,
  sandel.division,
  sandel.enduser,
  sandel.equivunit,
  sandel.familydescr,
  sandel.familyid,
  sandel.gbu,
  sandel.industry,
  sandel.materialsubtype,
  sandel.materialtype,
  sandel.postalcode,
  sandel.prodcategory,
  sandel.prodsubcategory,
  sandel.productstyle,
  sandel.qty,
  -- sandel.salesregion,
  zip3.sales_region salesregion,
  sandel.skudescr,
  sandel.skuid,
  sandel.state,
  sandel.subregion,
  COALESCE(
    CUST_OVERRIDE.MAPTOTERRITORY,
    PROD_OVERRIDE.MAPTOTERRITORY,
    ZIP3.TERRITORYID,
    ZIP0.TERRITORYID
  ) AS territory,
  sandel.tranamt,
  sandel.trandate trandate,
  sandel.USERID userid,
  sandel.vertical,
  ZIP3.sales_region osSalesRegion,
  COALESCE(
    THIRDORG_CUST_OVERRIDE.MAPTOTERRITORY,
    THIRDORG_PROD_OVERRIDE.MAPTOTERRITORY,
    THIRDORG_ZIP3.TERRITORYID
  ) osTerritory,
  (
    CASE
      WHEN (
        THIRDORG_CUST_OVERRIDE.MAPTOTERRITORY IS NOT NULL
        OR THIRDORG_PROD_OVERRIDE.MAPTOTERRITORY IS NOT NULL
      ) THEN NULL
      ELSE THIRDORG_ZIP3.USER_ID
    END
  ) osUserid,
  NULL marketlevel1,
  NULL marketlevel2,
  NULL marketlevel3,
  COALESCE(
    SECONDORG_CUST_OVERRIDE.MAPTOTERRITORY,
    SECONDORG_PROD_OVERRIDE.MAPTOTERRITORY,
    SECONDORG_ZIP3.TERRITORYID
  ) org2Territory,
  SECONDORG_ZIP3.sales_region org2SalesRegion,
  (
    CASE
      WHEN (
        SECONDORG_CUST_OVERRIDE.MAPTOTERRITORY IS NOT NULL
        OR SECONDORG_PROD_OVERRIDE.MAPTOTERRITORY IS NOT NULL
      ) THEN NULL
      ELSE SECONDORG_ZIP3.USER_ID
    END
  ) org2Userid
FROM
  main_stage sandel
  LEFT JOIN S_CORE.ACCOUNT_AGG ACCOUNT ON ACCOUNT.registrationId = (
    case
      when sandel.DistributorID like '% - OTD' then rtrim(' - OTD', sandel.DistributorID)
      when sandel.DistributorID like '% - ITD' then rtrim(' - ITD', sandel.DistributorID)
      else sandel.DistributorID
    end
  )
  LEFT JOIN S_CORE.PARTY_AGG PARTY ON PARTY.partynumber = (
    case
      when sandel.DistributorID like '% - OTD' then rtrim(' - OTD', sandel.DistributorID)
      when sandel.DistributorID like '% - ITD' then rtrim(' - ITD', sandel.DistributorID)
      else sandel.DistributorID
    end
  ) --LEFT JOIN S_CORE.organization_agg ORG ON upper(ORG.name) = upper(sandel.COMPANY)
  LEFT JOIN ZIP3 ON substr(sandel.PostalCode, 1, 3) = ZIP3.zip3
  AND sandel.vertical = ZIP3.vertical
  LEFT JOIN ZIP0 ON sandel.vertical = ZIP0.vertical
  LEFT JOIN SECONDORG_ZIP3 ON substr(sandel.PostalCode, 1, 3) = SECONDORG_ZIP3.zip3
  AND sandel.vertical = SECONDORG_ZIP3.vertical --LEFT JOIN SECONDORG_ZIP0 ON sandel.vertical                   = SECONDORG_ZIP0.vertical
  LEFT JOIN S_CORE.PRODUCT_EBS PROD ON sandel.skuid = PROD.PRODUCTCODE
  LEFT JOIN smartsheets.qvpos_product_territory_override PROD_OVERRIDE ON sandel.skuid = PROD_OVERRIDE.PRODUCTID
  LEFT JOIN CUST_OVERRIDE ON sandel.DistributorID = CUST_OVERRIDE.custid
  LEFT JOIN SECONDORG_PROD_OVERRIDE ON sandel.skuid = SECONDORG_PROD_OVERRIDE.PRODUCTID
  LEFT JOIN SECONDORG_CUST_OVERRIDE ON sandel.DistributorID = SECONDORG_CUST_OVERRIDE.custid
  LEFT JOIN TERR_DEFAULT ON sandel.vertical = TERR_DEFAULT.vertical
  AND SUBSTR(sandel.PostalCode, 1, 3) = TERR_DEFAULT.zip3
  LEFT JOIN THIRDORG_PROD_OVERRIDE ON sandel.skuid = THIRDORG_PROD_OVERRIDE.PRODUCTID
  LEFT JOIN THIRDORG_CUST_OVERRIDE ON sandel.DistributorID = THIRDORG_CUST_OVERRIDE.custid
  LEFT JOIN THIRDORG_ZIP3 ON substr(sandel.PostalCode, 1, 3) = THIRDORG_ZIP3.zip3 --WHERE ORG.organizationType = 'OPERATING_UNIT'

""")

main.createOrReplaceTempView('main')

# COMMAND ----------

# TRANSFORM DATA
columns = list(schema.keys())

main_f = (
  main
  .transform(parse_date(['trandate'], expected_format = 'MM/dd/yyyy'))
  .transform(parse_timestamp(['createdOn'], expected_format = 'MM/dd/yyyy')) 
  .transform(tg_default(source_name))
  .transform(tg_trade_management_point_of_sales_sandel())
  .transform(attach_dataset_column('SANDEL'))
  .transform(apply_schema(schema))
  .transform(attach_unknown_record)
  .select(columns)
  .transform(sort_columns)
)

# COMMAND ----------

# PERSIST DATA
options = {'target_storage': target_storage, 'target_container': target_container}
merge_to_delta(main_f, table_name, target_folder, overwrite, options = options)

# COMMAND ----------

# UPDATE FK
update_foreign_key(table_name, 'createdBy,_SOURCE', 'createdBy_ID', 'edm.user')
update_foreign_key(table_name, 'modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_trademanagement
