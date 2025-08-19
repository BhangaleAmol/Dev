# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_trademanagement

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_trademanagement.pricelistline

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'ebs.qp_list_lines', prune_days)
  main_inc = load_incr_dataset('ebs.qp_list_lines', 'LAST_UPDATE_DATE', cutoff_value)
else:
  main_inc = load_full_dataset('ebs.qp_list_lines')

# COMMAND ----------

# SAMPLING
if sampling:
  main_inc = main_inc.limit(10)

# COMMAND ----------

# VIEWS
main_inc.createOrReplaceTempView('qp_list_lines')

# COMMAND ----------

prod_attr=spark.sql("""
SELECT
  max(a.user_segment_name) user_segment_name,
  b.segment_mapping_column --NVL (a.user_segment_name, a.seeded_segment_name)
FROM
  ebs.qp_segments_tl a,
  ebs.qp_segments_b b,
  ebs.qp_prc_contexts_b c,
  ebs.qp_pte_segments d
WHERE
  c.prc_context_id = b.prc_context_id -- AND b.segment_mapping_column = l.product_attribute
  AND b.segment_id = a.segment_id
  AND a.language = 'US'
  AND b.segment_id = d.segment_id
group by
  b.segment_mapping_column
  """)
prod_attr .createOrReplaceTempView('prod_attr')

# COMMAND ----------

list_lines_v=spark.sql("""
select 
list_line_id,
max(PRODUCT_ATTR_VAL_DISP) PRODUCT_ATTR_VAL_DISP ,
    max(product_uom_code)  product_uom_code ,
    max(operand) operand ,
     max(start_date_active) start_date_active ,
    max(end_date_active) end_date_active ,
     max(list_header_id) list_header_id,
     max(PRODUCT_ATTRIBUTE) PRODUCT_ATTRIBUTE
     from (
select
   qppr.PRODUCT_ATTR_VAL_DISP ,
    qppr.product_uom_code ,
    qpll.operand ,
     qpll.start_date_active ,
    qpll.end_date_active ,
     qpll.list_header_id,
     qpll.list_line_id,
     QPPR.PRODUCT_ATTRIBUTE
FROM
    ebs.qp_list_lines qpll,
    ebs.qp_pricing_attributes qppr
WHERE
    qppr.list_line_id = qpll.list_line_id
    AND qpll.list_line_type_code IN (
        'PLL',
        'PBH'
    )
    AND qppr.pricing_phase_id = 1
    AND qppr.qualification_ind IN (
        4,
        6,
        20,
        22
    )
    AND qpll.pricing_phase_id = 1
    AND qpll.qualification_ind IN (
        4,
        6,
        20,
        22
    )
    AND qppr.list_header_id = qpll.list_header_id
AND ( ( qppr.rowid in (
    SELECT
         max(ROWID)
    FROM
        ebs.qp_pricing_attributes
    WHERE
        qppr.list_line_id = list_line_id
        AND pricing_attribute_context = 'PRICING ATTRIBUTE'
        AND pricing_attribute = 'PRICING_ATTRIBUTE11'
        --AND ROWNUM < 2
) )
OR (
    qppr.rowid in (
        SELECT
             ROWID
        FROM
            ebs.qp_pricing_attributes
        WHERE
            qppr.list_line_id = list_line_id
            AND pricing_attribute_context IS NULL
            AND pricing_attribute IS NULL
            AND excluder_flag = 'N'
    )
    AND NOT EXISTS (
        SELECT
            NULL
        FROM
            ebs.qp_pricing_attributes
        WHERE
            qppr.list_line_id = list_line_id
            AND pricing_attribute_context = 'PRICING ATTRIBUTE'
            AND pricing_attribute = 'PRICING_ATTRIBUTE11'
    )
) )
       ) group by list_line_id
""")
list_lines_v.createOrReplaceTempView('list_lines_v')

# COMMAND ----------

main = spark.sql("""
SELECT
REPLACE(STRING(INT (QP_LIST_LINES.CREATED_BY)), ",", "") AS createdBy,
QP_LIST_LINES.CREATION_DATE AS createdOn,
REPLACE(STRING(INT (QP_LIST_LINES.LAST_UPDATED_BY)), ",", "") AS modifiedBy,
QP_LIST_LINES.LAST_UPDATE_DATE AS modifiedOn,
CURRENT_DATE() AS insertedOn,
CURRENT_DATE() AS updatedOn,
QP_LIST_LINES.END_DATE_ACTIVE AS endDate,
QP_LIST_LINES.PERCENT_PRICE AS percentPrice,
QP_LIST_LINES.PRICE_BY_FORMULA_ID AS priceByFormulaId,
REPLACE(STRING(INT (QP_LIST_LINES.LIST_HEADER_ID)), ",", "") AS priceListHeaderId,
QP_LIST_LINES.ATTRIBUTE1 AS priceListlineAttribute1,
QP_LIST_LINES.ATTRIBUTE10 AS priceListlineAttribute10,
QP_LIST_LINES.ATTRIBUTE11 AS priceListlineAttribute11,
QP_LIST_LINES.ATTRIBUTE12 AS priceListlineAttribute12,
QP_LIST_LINES.ATTRIBUTE13 AS priceListlineAttribute13,
QP_LIST_LINES.ATTRIBUTE14 AS priceListlineAttribute14,
QP_LIST_LINES.ATTRIBUTE15 AS priceListlineAttribute15,
QP_LIST_LINES.ATTRIBUTE2 AS priceListlineAttribute2,
QP_LIST_LINES.ATTRIBUTE3 AS priceListlineAttribute3,
QP_LIST_LINES.ATTRIBUTE4 AS priceListlineAttribute4,
QP_LIST_LINES.ATTRIBUTE5 AS priceListlineAttribute5,
QP_LIST_LINES.ATTRIBUTE6 AS priceListlineAttribute6,
QP_LIST_LINES.ATTRIBUTE7 AS priceListlineAttribute7,
QP_LIST_LINES.ATTRIBUTE8 AS priceListlineAttribute8,
QP_LIST_LINES.ATTRIBUTE9 AS priceListlineAttribute9,
QP_LIST_LINES.COMMENTS AS priceListLineComment,
l.PRODUCT_ATTR_VAL_DISP AS priceListItemNumber,
MSIB.INVENTORY_ITEM_ID AS priceListItemId,
prod_attr.user_segment_name AS priceListProductLevel,
QP_LIST_LINES.OPERAND AS unitPrice,
l.product_uom_code AS productUomCode,
REPLACE(STRING(INT (QP_LIST_LINES.LIST_LINE_ID)), ",", "") AS priceListLineId,
QP_LIST_LINES.PRIMARY_UOM_FLAG AS primaryUomFlag,
QP_LIST_LINES.PROGRAM_APPLICATION_ID AS programApplicationId,
QP_LIST_LINES.PROGRAM_ID AS programId,
QP_LIST_LINES.PROGRAM_UPDATE_DATE AS programUpdateDate,
QP_LIST_LINES.START_DATE_ACTIVE AS startDate,
QP_LIST_LINES.LIST_PRICE AS unitListPrice

FROM 
QP_LIST_LINES

LEFT JOIN list_lines_v l ON QP_LIST_LINES.LIST_LINE_ID = l.LIST_LINE_ID
LEFT JOIN ebs.MTL_SYSTEM_ITEMS_B MSIB ON L.PRODUCT_ATTR_VAL_DISP = MSIB.SEGMENT1
LEFT JOIN prod_attr ON prod_attr.segment_mapping_column = l.product_attribute
WHERE 
MSIB.ORGANIZATION_ID = 124

""")

main.createOrReplaceTempView('main')

# COMMAND ----------

columns = list(schema.keys())
key_columns = ['priceListLineId']

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .transform(tg_default(source_name))
  .transform(tg_trade_management_price_list_line())
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

# HANDLE DELETE
full_keys_f = (
  spark.table('EBS.QP_LIST_LINES')
  .selectExpr('REPLACE(STRING(INT (QP_LIST_LINES.LIST_LINE_ID)), ",", "") AS priceListLineId')
  .transform(attach_source_column(source = source_name))
  .transform(attach_primary_key('priceListLineId,_SOURCE', 'edm.price_list_line'))
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
  update_cutoff_value(cutoff_value, table_name, 'ebs.qp_list_lines')
  update_run_datetime(run_datetime, table_name, 'ebs.qp_list_lines')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_trademanagement
