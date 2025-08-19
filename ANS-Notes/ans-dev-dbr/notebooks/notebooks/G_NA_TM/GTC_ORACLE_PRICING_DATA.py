# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ../_SCHEMA/g_na_tm.gtc_oracle_pricing_data

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_na_tm')
incremental = False
key_columns = get_input_param('key_columns', 'string', default_value = 'pl_name')
overwrite = True
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'gtc_oracle_pricing_data')
target_folder = get_input_param('target_folder', default_value = '/datalake_gold/na_tm/full_data')

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

list_lines_v=spark.sql("""
--select 
--max(PRODUCT_ATTR_VAL_DISP) PRODUCT_ATTR_VAL_DISP ,
  --  max(product_uom_code)  product_uom_code ,
    --max(operand) operand ,
     --max(start_date_active) start_date_active ,
    --max(end_date_active) end_date_active ,
     --list_header_id,
     --max(PRODUCT_ATTRIBUTE) PRODUCT_ATTRIBUTE
     --from (
select
   qppr.PRODUCT_ATTR_VAL_DISP ,
    qppr.product_uom_code ,
    qpll.operand ,
     qpll.start_date_active ,
    qpll.end_date_active ,
     qpll.list_header_id,
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
      -- ) group by list_header_id
""")
list_lines_v.createOrReplaceTempView('list_lines_v')

# COMMAND ----------

secondary_price_lists_v=spark.sql("""
select
    qpht.name,
    qpht.description,
    qphb.start_date_active,
    qphb.end_date_active,
    qphb.currency_code,
    qphb.list_header_id,
   QPQR.QUALIFIER_ATTR_VALUE parent_price_list_id,
    qpqr.qualifier_context,
    qpqr.qualifier_attribute
     
    FROM
    ebs.qp_list_headers_b qphb,
    ebs.qp_list_headers_tl qpht,
    ebs.qp_qualifiers qpqr
  WHERE
    qphb.list_header_id = qpht.list_header_id
    AND qpht.language = 'US'
    AND string(qphb.list_header_id) <> qpqr.qualifier_attr_value
    AND qphb.list_header_id = qpqr.list_header_id
    AND qphb.list_type_code = 'PRL'
    AND qpqr.qualifier_rule_id IS NULL
    """)
secondary_price_lists_v .createOrReplaceTempView('secondary_price_lists_v')

# COMMAND ----------

secondary_price_lists_v1=spark.sql("""
select * from secondary_price_lists_v where  (
    qualifier_context = 'MODLIST'
    AND qualifier_attribute = 'QUALIFIER_ATTRIBUTE4'
)
OR (
    qualifier_context = 'ORDER'
    AND qualifier_attribute = 'QUALIFIER_ATTRIBUTE11'
)
""")
secondary_price_lists_v1 .createOrReplaceTempView('secondary_price_lists_v1')

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

main = spark.sql("""
SELECT
  fl.territory_short_name country,
    hca.account_number account_number,
    hca.account_name account_name,
    case
    when h.name like '%_DS_%' then 'DS'
    when h.name like '%_WHS_%' then 'WHS' END shipment_type,
    hca.attribute13 division,
    hca.attribute10 cust_group,
    h.name pl_name,
    h.description pl_desc,
    h.start_date_active header_start_date,
    h.end_date_active header_end_date,
    h.currency_code currency,
    prod_attr.user_segment_name product_attribute,
    l.product_attr_val_disp item_code,
    prod.productStyle,
    l.product_uom_code uom,
    l.operand unit_price,
    l.start_date_active line_start_date,
    l.end_date_active line_end_date
FROM
    ebs.hz_parties hp
    join    ebs.hz_cust_accounts hca on hp.party_id = hca.party_id
   
   
   join ebs.fnd_territories_tl fl on fl.territory_code = hp.country
    left join secondary_price_lists_v1 h on h.parent_price_list_id = hca.price_list_id
   left join list_lines_v l on  h.list_header_id = l.list_header_id
   left join prod_attr on prod_attr.segment_mapping_column = l.product_attribute
   left join s_core.product_ebs prod on l.product_attr_val_disp = prod.productcode and not prod._deleted
WHERE
    
   fl.language = 'US'
    AND h.name LIKE 'ROA%'
     
    """)
main .createOrReplaceTempView('main')

# COMMAND ----------

# TRANSFORM DATA
main_f = (
   main
   .transform(attach_partition_column("line_start_date"))
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
