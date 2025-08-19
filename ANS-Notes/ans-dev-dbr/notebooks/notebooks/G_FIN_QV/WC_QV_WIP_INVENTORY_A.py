# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ../_SCHEMA/g_fin_qv.wc_qv_wip_inventory_a

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_fin_qv')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'string', default_value = 'INVOICE_DATE,ITEM_BRANCH_KEY')
overwrite = True
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'wc_qv_wip_inventory_a')
target_folder = get_input_param('target_folder', default_value = '/datalake_gold/fin_qv/full_data')

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
source_table = 'ebs.mtl_material_transactions'
if incremental:
  cutoff_value = get_cutoff_value(target_table, source_table, prune_days)  
  mtl_material_transactions = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  mtl_material_transactions = load_full_dataset(source_table)
  
mtl_material_transactions.createOrReplaceTempView('mtl_material_transactions')
mtl_material_transactions.display()

# COMMAND ----------

COST = spark.sql("""
 select
  GL_ITEM_CST.start_date,
  GL_ITEM_CST.end_date,
  GL_ITEM_CST.acctg_cost,
  GL_ITEM_CST.inventory_item_id,
  GL_ITEM_CST.organization_id,
  GL_ITEM_CST.period_id,
  std_cost.std_cost,
  freight.freight,
  duty.duty,
  mark_up.mark_up,
  NVL(std_cost.std_cost,0) + NVL(freight.freight,0) + NVL(duty.duty,0) SEE_THRU_COST,
  NVL(mark_up.mark_up,0) SGA_COST,
  GSOB.CURRENCY_CODE
from
  EBS.GL_ITEM_CST
  left outer join (
    SELECT
      CM_CMPT_DTL.INVENTORY_ITEM_ID,
      CM_CMPT_DTL.ORGANIZATION_ID,
      CM_CMPT_DTL.COST_TYPE_ID,
      CM_CMPT_DTL.PERIOD_ID,
      SUM(CM_CMPT_DTL.CMPNT_COST) STD_COST
    FROM
      EBS.CM_CMPT_DTL
      INNER JOIN EBS.CM_CMPT_MST_B ON CM_CMPT_DTL.COST_CMPNTCLS_ID = CM_CMPT_MST_B.COST_CMPNTCLS_ID
    WHERE
      CM_CMPT_DTL.COST_TYPE_ID = 1000
      AND CM_CMPT_MST_B.COST_CMPNTCLS_CODE NOT IN (
        'DUTY',
        'DUTY-ADJ',
        'MEDICALTAX',
        'INFREIGHT',
        'INFREIGHT-ADJ',
        'SGA',
        'MUPADJ',
        'MARK UP',
        'SGA-ADJ'
      )
    GROUP BY
      CM_CMPT_DTL.INVENTORY_ITEM_ID,
      CM_CMPT_DTL.ORGANIZATION_ID,
      CM_CMPT_DTL.COST_TYPE_ID,
      CM_CMPT_DTL.PERIOD_ID
  ) std_cost on GL_ITEM_CST.inventory_item_id = std_cost.inventory_item_id
  and GL_ITEM_CST.organization_id = std_cost.organization_id
  and GL_ITEM_CST.period_id = std_cost.period_id
  left outer join (
    SELECT
      CM_CMPT_DTL.INVENTORY_ITEM_ID,
      CM_CMPT_DTL.ORGANIZATION_ID,
      CM_CMPT_DTL.COST_TYPE_ID,
      CM_CMPT_DTL.PERIOD_ID,
      SUM(CM_CMPT_DTL.CMPNT_COST) freight
    FROM
      EBS.CM_CMPT_DTL
      INNER JOIN EBS.CM_CMPT_MST_B ON CM_CMPT_DTL.COST_CMPNTCLS_ID = CM_CMPT_MST_B.COST_CMPNTCLS_ID
    WHERE
      CM_CMPT_DTL.COST_TYPE_ID = 1000
      AND CM_CMPT_MST_B.COST_CMPNTCLS_CODE IN ('INFREIGHT', 'INFREIGHT-ADJ')
    GROUP BY
      CM_CMPT_DTL.INVENTORY_ITEM_ID,
      CM_CMPT_DTL.ORGANIZATION_ID,
      CM_CMPT_DTL.COST_TYPE_ID,
      CM_CMPT_DTL.PERIOD_ID
  ) freight on GL_ITEM_CST.inventory_item_id = freight.inventory_item_id
  and GL_ITEM_CST.organization_id = freight.organization_id
  and GL_ITEM_CST.period_id = freight.period_id
  left outer join (
    SELECT
      CM_CMPT_DTL.INVENTORY_ITEM_ID,
      CM_CMPT_DTL.ORGANIZATION_ID,
      CM_CMPT_DTL.COST_TYPE_ID,
      CM_CMPT_DTL.PERIOD_ID,
      SUM(CM_CMPT_DTL.CMPNT_COST) duty
    FROM
      EBS.CM_CMPT_DTL
      INNER JOIN EBS.CM_CMPT_MST_B ON CM_CMPT_DTL.COST_CMPNTCLS_ID = CM_CMPT_MST_B.COST_CMPNTCLS_ID
    WHERE
      CM_CMPT_DTL.COST_TYPE_ID = 1000
      AND CM_CMPT_MST_B.COST_CMPNTCLS_CODE IN ('DUTY', 'DUTY-ADJ', 'MEDICALTAX')
    GROUP BY
      CM_CMPT_DTL.INVENTORY_ITEM_ID,
      CM_CMPT_DTL.ORGANIZATION_ID,
      CM_CMPT_DTL.COST_TYPE_ID,
      CM_CMPT_DTL.PERIOD_ID
  ) duty on GL_ITEM_CST.inventory_item_id = duty.inventory_item_id
  and GL_ITEM_CST.organization_id = duty.organization_id
  and GL_ITEM_CST.period_id = duty.period_id
  left outer join (
    SELECT
      CM_CMPT_DTL.INVENTORY_ITEM_ID,
      CM_CMPT_DTL.ORGANIZATION_ID,
      CM_CMPT_DTL.COST_TYPE_ID,
      CM_CMPT_DTL.PERIOD_ID,
      SUM(CM_CMPT_DTL.CMPNT_COST) mark_up
    FROM
      EBS.CM_CMPT_DTL
      INNER JOIN EBS.CM_CMPT_MST_B ON CM_CMPT_DTL.COST_CMPNTCLS_ID = CM_CMPT_MST_B.COST_CMPNTCLS_ID
    WHERE
      CM_CMPT_DTL.COST_TYPE_ID = 1000
      AND CM_CMPT_MST_B.COST_CMPNTCLS_CODE IN ('SGA', 'MUPADJ', 'MARK UP', 'SGA-ADJ')
    GROUP BY
      CM_CMPT_DTL.INVENTORY_ITEM_ID,
      CM_CMPT_DTL.ORGANIZATION_ID,
      CM_CMPT_DTL.COST_TYPE_ID,
      CM_CMPT_DTL.PERIOD_ID
  ) mark_up on GL_ITEM_CST.inventory_item_id = mark_up.inventory_item_id
  and GL_ITEM_CST.organization_id = mark_up.organization_id
  and GL_ITEM_CST.period_id = mark_up.period_id
  LEFT OUTER JOIN  EBS.HR_ORGANIZATION_INFORMATION HOU on GL_ITEM_CST.organization_id = HOU.organization_id
  AND  HOU.ORG_INFORMATION_CONTEXT = 'Accounting Information'
  INNER JOIN EBS.HR_OPERATING_UNITS HO ON HOU.ORG_INFORMATION3 = HO.ORGANIZATION_ID
  INNER JOIN EBS.GL_SETS_OF_BOOKS GSOB ON HO.SET_OF_BOOKS_ID = GSOB.SET_OF_BOOKS_ID
where
  GL_ITEM_CST.COST_TYPE_ID = 1000
    """)

COST.createOrReplaceTempView("COST")

# COMMAND ----------

parent_loc = spark.sql("""
SELECT XLE_ENTITY_PROFILES.NAME, HR_ORGANIZATION_INFORMATION.ORGANIZATION_ID 
FROM
 EBS.XLE_ENTITY_PROFILES, EBS.HR_ORGANIZATION_INFORMATION 
WHERE
 HR_ORGANIZATION_INFORMATION.ORG_INFORMATION_CONTEXT IN ('Accounting Information') 
AND
 XLE_ENTITY_PROFILES.LEGAL_ENTITY_ID = HR_ORGANIZATION_INFORMATION.ORG_INFORMATION2
  """)

parent_loc.createOrReplaceTempView("parent_loc")

# COMMAND ----------

main_wip_inventory = spark.sql("""
SELECT
  MMT.ORGANIZATION_ID,
  org.organizationCode ITEM_BRANCH_KEY,
  CASE
      WHEN parent_loc.name  = 'Ansell  Mexico'
      THEN '5100'
      WHEN parent_loc.name  = 'Ansell Canada Inc'
      THEN '5000'
      WHEN parent_loc.name  = 'Ansell Hawkeye Inc'
      THEN '2020'
      WHEN parent_loc.name  = 'Ansell Healthcare Products LLC'
      THEN '2010'
      WHEN parent_loc.name  = 'Ansell Protective Products Inc'
      THEN '2000'
     WHEN parent_loc.name  = 'SXWELL USA LLC'
      THEN '2190'
      ELSE parent_loc.name 
    END  CMP_NUM_DETAIL,
  date.monthEndDate SHIP_DATE,
  date.monthEndDate INVOICE_DATE,
  prod.productcode ITEM_NUMBER,
  prod.ansStdUom ANSELL_STD_UM,
  SUM(
    CASE
      WHEN (
        case
          when mmt.TRANSACTION_QUANTITY < 0 then 'C'
          else (
            case
              when mmt.TRANSACTION_QUANTITY > 0
              or mmt.TRANSACTION_QUANTITY = 0 then 'D'
            END
          )
        END
      ) = 'C' THEN CASE
        WHEN mmt.TRANSACTION_UOM = prod.primarySellingUom THEN mmt.TRANSACTION_QUANTITY * -1
        WHEN mmt.TRANSACTION_UOM = prod.ansStdUom THEN mmt.TRANSACTION_QUANTITY / prod.ansStdUomConv * -1
        ELSE mmt.TRANSACTION_QUANTITY * -1
      END
      ELSE 0
    END
  ) SALES_QUANTITY,
  SUM(
    CASE
      WHEN (
        case
          when mmt.TRANSACTION_QUANTITY < 0 then 'C'
          else (
            case
              when mmt.TRANSACTION_QUANTITY > 0
              or mmt.TRANSACTION_QUANTITY = 0 then 'D'
            END
          )
        END
      ) = 'C' THEN (
        CASE
          WHEN mmt.TRANSACTION_COST IS NULL THEN mmt.ACTUAL_COST * mmt.TRANSACTION_QUANTITY
          ELSE mmt.TRANSACTION_COST * mmt.TRANSACTION_QUANTITY
        END
      ) * -1
      ELSE 0
    END
  ) GROSS_SALES_AMOUNT_LC,
  SUM(
    CASE
      WHEN (
        case
          when mmt.TRANSACTION_QUANTITY < 0 then 'C'
          else (
            case
              when mmt.TRANSACTION_QUANTITY > 0
              or mmt.TRANSACTION_QUANTITY = 0 then 'D'
            END
          )
        END
      ) = 'D' THEN (
        CASE
          WHEN mmt.TRANSACTION_COST IS NULL THEN mmt.ACTUAL_COST * mmt.TRANSACTION_QUANTITY
          ELSE mmt.TRANSACTION_COST * mmt.TRANSACTION_QUANTITY
        END
      )
      ELSE 0
    END
  ) RETURNS_LC,
  SUM(
    CASE
      WHEN mmt.TRANSACTION_COST IS NULL THEN mmt.ACTUAL_COST * mmt.TRANSACTION_QUANTITY
      ELSE mmt.TRANSACTION_COST * mmt.TRANSACTION_QUANTITY
    END
  ) NET_SALES_LC,
  SUM(
    CASE
      WHEN (
        case
          when mmt.TRANSACTION_QUANTITY < 0 then 'C'
          else (
            case
              when mmt.TRANSACTION_QUANTITY > 0
              or mmt.TRANSACTION_QUANTITY = 0 then 'D'
            END
          )
        END
      ) = 'C' THEN CASE
        when mmt.TRANSACTION_UOM = prod.primarySellingUom THEN mmt.TRANSACTION_QUANTITY * COST.SEE_THRU_COST * -1
        when mmt.TRANSACTION_UOM = prod.ansStdUom THEN mmt.TRANSACTION_QUANTITY / prod.ansStdUomConv * COST.SEE_THRU_COST * -1
        ELSE mmt.TRANSACTION_QUANTITY * -1 * COST.SEE_THRU_COST
      END
      ELSE 0
    END
  ) SALES_COST_AMOUNT_LC,
  CASE
    WHEN MMT.TRANSACTION_TYPE_ID = 33 THEN 'Ship Confirm external Sales Order'
    WHEN MMT.TRANSACTION_TYPE_ID = 34 THEN 'Ship Confirm Internal Order: Issue'
    WHEN MMT.TRANSACTION_TYPE_ID = 35 THEN 'WIP Issue'
  END DOC_TYPE,
  CASE
    WHEN MMT.TRANSACTION_TYPE_ID = 33 THEN 'Ship Confirm external Sales Order'
    WHEN MMT.TRANSACTION_TYPE_ID = 34 THEN 'Ship Confirm Internal Order: Issue'
    WHEN MMT.TRANSACTION_TYPE_ID = 35 THEN 'WIP Issue'
  END TRANS_TYPE,
  SUM(
    CASE
      WHEN (
        case
          when mmt.TRANSACTION_QUANTITY < 0 then 'C'
          else (
            case
              when mmt.TRANSACTION_QUANTITY > 0
              or mmt.TRANSACTION_QUANTITY = 0 then 'D'
            END
          )
        END
      ) = 'C' THEN CASE
        when mmt.TRANSACTION_UOM = prod.primarySellingUom THEN mmt.TRANSACTION_QUANTITY *(
          COST.SGA_COST + COST.SEE_THRU_COST
        ) * -1
        when mmt.TRANSACTION_UOM = prod.ansStdUom THEN mmt.TRANSACTION_QUANTITY / prod.ansStdUomConv *(COST.SGA_COST + COST.SEE_THRU_COST) * -1
        ELSE mmt.TRANSACTION_QUANTITY *(COST.SGA_COST + COST.SEE_THRU_COST) * -1
      END
      ELSE 0
    END
  ) SALES_COST_AMOUNT_LE 
  FROM
  EBS.MTL_MATERIAL_TRANSACTIONS MMT
  LEFT JOIN EBS.HR_ORGANIZATION_INFORMATION HROI ON MMT.ORGANIZATION_ID = HROI.ORGANIZATION_ID
  AND HROI.ORG_INFORMATION_CONTEXT = 'Accounting Information'
  LEFT JOIN EBS.MTL_SYSTEM_ITEMS_B MSIB ON MMT.INVENTORY_ITEM_ID = MSIB.INVENTORY_ITEM_ID
  AND MMT.ORGANIZATION_ID = MSIB.ORGANIZATION_ID
  LEFT JOIN EBS.RCV_TRANSACTIONS RCV ON MMT.RCV_TRANSACTION_ID = RCV.TRANSACTION_ID
  LEFT JOIN EBS.PO_HEADERS_ALL POH ON RCV.PO_HEADER_ID = POH.PO_HEADER_ID
  LEFT JOIN EBS.PO_LINES_ALL POL ON RCV.PO_LINE_ID = POL.PO_LINE_ID
  LEFT JOIN EBS.OE_ORDER_LINES_ALL OOLA ON MMT.TRX_SOURCE_LINE_ID = OOLA.LINE_ID
  LEFT JOIN EBS.OE_ORDER_HEADERS_ALL OOHA ON OOLA.HEADER_ID = OOHA.HEADER_ID
  LEFT JOIN S_CORE.DATE DATE ON DATE_FORMAT(MMT.TRANSACTION_DATE, 'yyyyMMdd') = date.dayid
  LEFT JOIN S_CORE.PRODUCT_EBS PROD ON PROD.ITEMID = MMT.INVENTORY_ITEM_ID
  LEFT JOIN s_core.organization_ebs org on mmt.ORGANIZATION_ID = org.organizationId
  --LEFT JOIN S_CORE.TRANSACTION_TYPE_EBS ON TRANSACTION_TYPE_ID
  LEFT JOIN COST ON MMT.INVENTORY_ITEM_ID = COST.INVENTORY_ITEM_ID
  AND MMT.organization_id = COST.organization_id
  AND date.monthEndDate BETWEEN COST.START_DATE
  AND COST.END_DATE
  LEFT JOIN parent_loc ON MMT.ORGANIZATION_ID = parent_loc.ORGANIZATION_ID
WHERE
  MMT.TRANSACTION_TYPE_ID IN (33, 34, 35)
  AND MMT.ORGANIZATION_ID IN (607, 2974)
  GROUP BY
  MMT.ORGANIZATION_ID,
  org.organizationCode ,
  date.monthEndDate ,
  date.monthEndDate ,
  prod.productcode ,
  prod.ansStdUom ,
   CASE
    WHEN MMT.TRANSACTION_TYPE_ID = 33 THEN 'Ship Confirm external Sales Order'
    WHEN MMT.TRANSACTION_TYPE_ID = 34 THEN 'Ship Confirm Internal Order: Issue'
    WHEN MMT.TRANSACTION_TYPE_ID = 35 THEN 'WIP Issue'
  END,
   CASE
      WHEN parent_loc.name  = 'Ansell  Mexico'
      THEN '5100'
      WHEN parent_loc.name  = 'Ansell Canada Inc'
      THEN '5000'
      WHEN parent_loc.name  = 'Ansell Hawkeye Inc'
      THEN '2020'
      WHEN parent_loc.name  = 'Ansell Healthcare Products LLC'
      THEN '2010'
      WHEN parent_loc.name  = 'Ansell Protective Products Inc'
      THEN '2000'
     WHEN parent_loc.name  = 'SXWELL USA LLC'
      THEN '2190'
      ELSE parent_loc.name 
    END
  
  """)
main_wip_inventory.createOrReplaceTempView("main_wip_inventory")

# COMMAND ----------

# TRANSFORM DATA
main_f = (
   main_wip_inventory
   .transform(attach_partition_column("INVOICE_DATE"))
   .transform(attach_modified_date())
   .transform(attach_deleted_flag())
   .transform(apply_schema(schema))
)

if incremental:
  main_f.display()

# COMMAND ----------

# VALIDATE DATA
#if incremental:
 # check_distinct_count(main_f, key_columns)

# COMMAND ----------

# LOAD
options = {'overwrite': overwrite, 'partition_column': '_PART'}
register_hive_table(main_f, target_table, target_folder, options = options)
merge_into_table(main_f, target_table, key_columns, options = {'auto_merge': True})
