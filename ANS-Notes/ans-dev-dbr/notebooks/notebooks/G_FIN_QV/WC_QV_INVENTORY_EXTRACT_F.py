# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ../_SCHEMA/g_fin_qv.wc_qv_inventory_extract_f

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_fin_qv')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'string', default_value = 'INTEGRATION_ID')
overwrite = False
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'wc_qv_inventory_extract_f')
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
source_table = 'ebs.mtl_onhand_quantities_detail'
if incremental:
  cutoff_value = get_cutoff_value(target_table, source_table, prune_days)  
  mtl_onhand_quantities_detail = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  mtl_onhand_quantities_detail = load_full_dataset(source_table)
  
mtl_onhand_quantities_detail.createOrReplaceTempView('mtl_onhand_quantities_detail')
mtl_onhand_quantities_detail.display()

# COMMAND ----------

main_wc_qv_inventory_extract_f = spark.sql("""
SELECT
  LAST_DAY(CURRENT_DATE -1) MONTH_END_DATE,
  ORG.organizationCode INVENTORY_ORG,
  ORG.name INVENTORY_ORG_NAME,
  OU.NAME OPERATING_UNIT,
  --REPLACE(STRING(INT(HR_OPERATING_UNITS.ORGANIZATION_ID)), ",", "")     owningBusinessUnitId,
  --MOQD.INVENTORY_ITEM_ID,
  --MOQD.ORGANIZATION_ID,
  PROD.PRODUCTDIVISION DIVISION,
  PROD.PRODUCTCODE ITEM_NUMBER,
  PROD.name ITEM_DESCRIPTION,
  PROD.legacyAspn LEGACY_SPIN,
  PROD.launchDate LAUNCH_DATE,
  PROD.ANSSTDUOM STD_UOM,
  MOQD.SUBINVENTORY_CODE SUB_INVENTORY,
  MOQD.LOT_NUMBER LOT_NUMBER,
  SUM(
    CASE
      WHEN MOQD.IS_CONSIGNED = 2 THEN MOQD.PRIMARY_TRANSACTION_QUANTITY
      ELSE 0
    END
  ) ON_HAND_QTY_PRIMARY,
  SUM(
    CASE
      WHEN MOQD.IS_CONSIGNED = 2 THEN MOQD.PRIMARY_TRANSACTION_QUANTITY
      ELSE 0
    END * PROD.ansStduomConv
  ) ON_HAND_QTY,
  SUM(
    NVL(COSTS.SEE_THRU_COST, 0) * CASE
      WHEN MOQD.IS_CONSIGNED = 2 THEN MOQD.PRIMARY_TRANSACTION_QUANTITY
      ELSE 0
    END
  ) ON_HAND_SEE_THROUGH_COST,
  SUM(
    NVL(COSTS.LE_COST, 0) * CASE
      WHEN MOQD.IS_CONSIGNED = 2 THEN MOQD.PRIMARY_TRANSACTION_QUANTITY
      ELSE 0
    END
  ) ON_HAND_LE_COST,
  NULL ROW_WID,
  INT(MOQD.INVENTORY_ITEM_ID) || '-' || INT(MOQD.ORGANIZATION_ID) || '-' || MOQD.SUBINVENTORY_CODE || '-' || NVL(MOQD.LOT_NUMBER,'unknown') || '-' || date_format(LAST_DAY(CURRENT_DATE -1), 'yyyyMM') INTEGRATION_ID,
  NULL AS DATASOURCE_NUM_ID,
  NULL AS ETL_PROC_WID,
  CURRENT_DATE W_INSERT_DT,
  CURRENT_DATE W_UPDATE_DT,
  PROD.primarySellingUOM PRIMARY_UOM_CODE,
  MMS.STATUS_CODE SUBINVENTORY_STATUS,
  LOT.EXPIRATION_DATE LOT_EXPIRY_DATE
FROM
  EBS.MTL_ONHAND_QUANTITIES_DETAIL MOQD
  JOIN EBS.MTL_SECONDARY_INVENTORIES IMS ON MOQD.SUBINVENTORY_CODE = IMS.SECONDARY_INVENTORY_NAME
  AND MOQD.ORGANIZATION_ID = IMS.ORGANIZATION_ID
  LEFT JOIN EBS.MTL_MATERIAL_STATUSES_TL MMS ON IMS.STATUS_ID = MMS.STATUS_ID
  LEFT JOIN EBS.MTL_LOT_NUMBERS LOT ON MOQD.ORGANIZATION_ID = LOT.ORGANIZATION_ID
  AND MOQD.INVENTORY_ITEM_ID = LOT.INVENTORY_ITEM_ID
  AND MOQD.LOT_NUMBER = LOT.LOT_NUMBER
  LEFT JOIN S_CORE.PRODUCT_EBS PROD ON PROD.ITEMID = MOQD.INVENTORY_ITEM_ID
  LEFT JOIN S_CORE.ORGANIZATION_EBS ORG ON ORG.ORGANIZATIONID = MOQD.ORGANIZATION_ID
  INNER JOIN EBS.HR_ORGANIZATION_INFORMATION ON MOQD.ORGANIZATION_ID = HR_ORGANIZATION_INFORMATION.ORGANIZATION_ID
  INNER JOIN EBS.HR_OPERATING_UNITS ON HR_ORGANIZATION_INFORMATION.ORG_INFORMATION3 = HR_OPERATING_UNITS.ORGANIZATION_ID
  LEFT JOIN S_CORE.ORGANIZATION_EBS OU ON OU.ORGANIZATIONID = HR_OPERATING_UNITS.ORGANIZATION_ID
  LEFT JOIN (
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
      std_cost.std_cost + freight.freight + duty.duty SEE_THRU_COST,
      std_cost.std_cost + freight.freight + duty.duty + mark_up.mark_up LE_COST,
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
      LEFT OUTER JOIN EBS.HR_ORGANIZATION_INFORMATION HOU on GL_ITEM_CST.organization_id = HOU.organization_id
      AND HOU.ORG_INFORMATION_CONTEXT = 'Accounting Information'
      INNER JOIN EBS.HR_OPERATING_UNITS HO ON HOU.ORG_INFORMATION3 = HO.ORGANIZATION_ID
      INNER JOIN EBS.GL_SETS_OF_BOOKS GSOB ON HO.SET_OF_BOOKS_ID = GSOB.SET_OF_BOOKS_ID
    where
      GL_ITEM_CST.COST_TYPE_ID = 1000
      AND GL_ITEM_CST.START_DATE <= CURRENT_DATE -1
  AND GL_ITEM_CST.END_DATE >= CURRENT_DATE -1
  ) COSTS ON MOQD.INVENTORY_ITEM_ID = COSTS.INVENTORY_ITEM_ID
  AND MOQD.ORGANIZATION_ID = COSTS.organization_id
WHERE
  MMS.language = 'US'
  and PROD._DELETED = 'false'
  AND org.organizationType = 'INV'
  AND OU.organizationType = 'OPERATING_UNIT'
  and HR_ORGANIZATION_INFORMATION.ORG_INFORMATION_CONTEXT = 'Accounting Information'
  and INT(MOQD.INVENTORY_ITEM_ID) || '-' || INT(MOQD.ORGANIZATION_ID) || '-' || MOQD.SUBINVENTORY_CODE || '-' || NVL(MOQD.LOT_NUMBER,'unknown')  is not null
GROUP BY
  --MOQD.INVENTORY_ITEM_ID,
  --MOQD.ORGANIZATION_ID,
  MOQD.SUBINVENTORY_CODE,
  MOQD.LOT_NUMBER,
  MMS.STATUS_CODE,
  LOT.EXPIRATION_DATE,
  PROD.PRODUCTDIVISION,
  PROD.PRODUCTCODE,
  PROD.name,
  PROD.legacyAspn,
  PROD.launchDate,
  PROD.primarysellinguom,
  ORG.organizationCode,
  ORG.name,
  OU.NAME,
  PROD.ANSSTDUOM,
  INT(MOQD.INVENTORY_ITEM_ID) || '-' || INT(MOQD.ORGANIZATION_ID) || '-' || MOQD.SUBINVENTORY_CODE || '-' || NVL(MOQD.LOT_NUMBER,'unknown') || '-' || date_format(LAST_DAY(CURRENT_DATE -1), 'yyyyMM')
  
  """)
main_wc_qv_inventory_extract_f.createOrReplaceTempView("main_wc_qv_inventory_extract_f")

# COMMAND ----------

# TRANSFORM DATA
main_f = (
   main_wc_qv_inventory_extract_f
   .transform(attach_partition_column("MONTH_END_DATE"))
   .transform(attach_modified_date())
   .transform(attach_deleted_flag())
   .transform(apply_schema(schema))
)

if incremental:
  main_f.display()

# COMMAND ----------

# VALIDATE DATA
# if incremental:
# check_distinct_count(main_f, key_columns)

# COMMAND ----------

# LOAD
options = {'overwrite': overwrite, 'partition_column': '_PART'}
register_hive_table(main_f, target_table, target_folder, options = options)
merge_into_table(main_f, target_table, key_columns, options = {'auto_merge': True})

# COMMAND ----------

# HANDLE DELETE
full_keys_f = (
  spark.sql("""
    SELECT
     INT(MOQD.INVENTORY_ITEM_ID) || '-' || INT(MOQD.ORGANIZATION_ID) || '-' || MOQD.SUBINVENTORY_CODE || '-' || NVL(MOQD.LOT_NUMBER,'unknown') || '-' || date_format(LAST_DAY(CURRENT_DATE -1), 'yyyyMM') INTEGRATION_ID
    FROM ebs.mtl_onhand_quantities_detail MOQD
  """)
)

# COMMAND ----------

filter_date = str(spark.sql('SELECT LAST_DAY(CURRENT_DATE-1) AS inventoryDate').collect()[0]['inventoryDate'])
options =  {'date_field' : 'MONTH_END_DATE', 'date_value' : filter_date}
apply_hard_delete(full_keys_f, table_name, key_columns = 'INTEGRATION_ID', options = options)

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not sampling:
  cutoff_value = get_max_value(mtl_onhand_quantities_detail, '_MODIFIED')
  update_cutoff_value(cutoff_value, target_table, 'ebs.mtl_onhand_quantities_detail')
  update_run_datetime(run_datetime, target_table, 'ebs.mtl_onhand_quantities_detail')

# COMMAND ----------

# MAGIC %sql
# MAGIC select -- ORGANIZATION_ID,
# MAGIC
# MAGIC SUM(
# MAGIC     CASE
# MAGIC       WHEN MOQD.IS_CONSIGNED = 2 THEN MOQD.PRIMARY_TRANSACTION_QUANTITY
# MAGIC       ELSE 0
# MAGIC     END * PROD.ansStduomConv
# MAGIC   ) ON_HAND_QTY   from ebs.mtl_onhand_quantities_detail moqd
# MAGIC   left join s_core.product_ebs prod on moqd.inventory_item_id = prod.itemid
# MAGIC   
# MAGIC   where  INVENTORY_ITEM_ID in (select inventory_item_id from ebs.mtl_system_items_b where segment1 in ('163164','163165','163166','163168','871079','871081','871082'))
# MAGIC   and INT(MOQD.INVENTORY_ITEM_ID) || '-' || INT(MOQD.ORGANIZATION_ID) || '-' || MOQD.SUBINVENTORY_CODE || '-' || NVL(MOQD.LOT_NUMBER,'unknown')  is not null
# MAGIC   --group by ORGANIZATION_ID

# COMMAND ----------

# MAGIC %sql
# MAGIC select  sum(on_hand_qty),month_end_date from g_fin_qv.wc_qv_inventory_extract_f where 
# MAGIC item_number in ('163164','163165','163166','163168','871079','871081','871082')
# MAGIC group by month_end_date
# MAGIC --and inventory_org in ('403','803','804','822','823','828') --and _deleted = 'false'
