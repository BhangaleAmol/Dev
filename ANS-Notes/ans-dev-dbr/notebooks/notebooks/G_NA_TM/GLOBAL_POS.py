# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ../_SCHEMA/g_na_tm.global_pos

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold" , -1)
spark.conf.set('spark.sql.adaptive.enabled', 'false')

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_na_tm')
incremental = get_input_param('incremental', 'bool', default_value = True)
key_columns = get_input_param('key_columns', 'list', default_value = ['_ID'])
overwrite = get_input_param('overwrite', 'bool', default_value = False)
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'global_pos')
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

# EXTRACT POS
source_table = 'g_na_tm.wc_qv_pos_a'
if incremental:
  cutoff_value = get_cutoff_value(target_table, source_table, prune_days)  
  wc_qv_pos_a = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  wc_qv_pos_a = load_full_dataset(source_table)
  
wc_qv_pos_a.createOrReplaceTempView('wc_qv_pos_a')
wc_qv_pos_a.display()

# COMMAND ----------

# EXTRACT POS
source_table = 'g_na_tm.wc_qv_pos_dis_a'
if incremental:
  cutoff_value = get_cutoff_value(target_table, source_table, prune_days)  
  wc_qv_pos_dis_a = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  wc_qv_pos_dis_a = load_full_dataset(source_table)
  
wc_qv_pos_dis_a.createOrReplaceTempView('wc_qv_pos_dis_a')
wc_qv_pos_dis_a.display()

# COMMAND ----------

# EXTRACT POS
source_table = 'g_na_tm.wc_qv_pos_manadj_a'
if incremental:
  cutoff_value = get_cutoff_value(target_table, source_table, prune_days)  
  wc_qv_pos_manadj_a = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  wc_qv_pos_manadj_a = load_full_dataset(source_table)
  
wc_qv_pos_manadj_a.createOrReplaceTempView('wc_qv_pos_manadj_a')
wc_qv_pos_manadj_a.display()

# COMMAND ----------

# SAMPLING
if sampling:  
  wc_qv_pos_a = wc_qv_pos_a.limit(10)
  wc_qv_pos_a.createOrReplaceTempView('wc_qv_pos_a')

# COMMAND ----------

# SAMPLING
if sampling:  
  wc_qv_pos_dis_a = wc_qv_pos_dis_a.limit(10)
  wc_qv_pos_dis_a.createOrReplaceTempView('wc_qv_pos_dis_a')

# COMMAND ----------

# SAMPLING
if sampling:  
  wc_qv_pos_manadj_a = wc_qv_pos_manadj_a.limit(10)
  wc_qv_pos_manadj_a.createOrReplaceTempView('wc_qv_pos_manadj_a')

# COMMAND ----------

main_pos_ebs=spark.sql("""
select
  pos._ID,
  'POS' SOURCE,
  ACCOUNT_ID,
  DISTRIBUTORID,
  product_qv._id ITEM_ID,
  prod.productcode ITEMID,
  TRANDATE,
  date_format(TRANDATE, 'yyyyMMdd') TRANS_DT_ID,
  sum(QTY) QTY_IN_CASES,
  SUM(QTY * PROD.ansStdUomConv) QTY_ANS_STD_UOM_CONV,
  SUM(TRANAMT) TRANAMT,
  concat(
    nvl(
      case
        when nvl(lower(TERRITORY), 'unknown') = 'unknown' then 'unknown'
        else TERRITORY
      end,
      'unknown'
    ),
    '-',
    nvl(SALESREGION, 'unknown'),
    '-',
    nvl(lower(USERID), 'unknown')
  ) TERRITORY_ID,
  concat(
    nvl(SECONDORG_TERRITORY, 'unknown'),
    '-',
    nvl(SECONDORG_SALESREGION, 'unknown'),
    '-',
    nvl(lower(SECONDORG_USERID), 'unknown')
  ) SECONDORG_TERRITORY_ID,
  UPPER(
    concat(
      nvl(
        nvl(Final_Lookup_canada.state, pos.state),
        'unknown'
      ),
      '-',
      nvl(nvl(primary_city, END_USER_CITY), 'unknown'),
      '-',
      nvl(
        nvl(
          CASE
            WHEN pos.SUBREGION = 'US' THEN SUBSTR(regexp_replace(pos.postalcode, ' ', ''), 1, 5)
            ELSE SUBSTR(regexp_replace(pos.postalcode, ' ', ''), 1, 6)
          END,
          Final_Lookup_canada.Postalcode
        ),
        'unknown'
      )
    )
  ) LOCATION_ID,
  'EBS' POSSOURCE,
  case
    when DISTRIBUTORID like '% - OTD' then 'OTD'
    when DISTRIBUTORID like '% - ITD' then 'ITD'
  end ITD_OTD_FLAG
from
  wc_qv_pos_a pos
  left join s_core.product_ebs prod on prod.productcode = pos.sku
  left join (
    select
      distinct *
    from
      amazusftp1.Final_Lookup_canada
  ) Final_Lookup_canada ON CASE
    WHEN pos.SUBREGION = 'US' THEN SUBSTR(regexp_replace(pos.postalcode, ' ', ''), 1, 5)
    ELSE SUBSTR(regexp_replace(pos.postalcode, ' ', ''), 1, 6)
  END = regexp_replace(Final_Lookup_canada.Postalcode, ' ', '')
  left join s_core.product_qv on prod.productcode = product_qv.itemid
where
  pos._deleted = 'false'
GROUP BY
  ACCOUNT_ID,
  DISTRIBUTORID,
  prod.productcode,
  TRANDATE,
  date_format(TRANDATE, 'yyyyMMdd'),
  concat(
    nvl(
      case
        when nvl(lower(TERRITORY), 'unknown') = 'unknown' then 'unknown'
        else TERRITORY
      end,
      'unknown'
    ),
    '-',
    nvl(SALESREGION, 'unknown'),
    '-',
    nvl(lower(USERID), 'unknown')
  ),
  concat(
    nvl(SECONDORG_TERRITORY, 'unknown'),
    '-',
    nvl(SECONDORG_SALESREGION, 'unknown'),
    '-',
    nvl(lower(SECONDORG_USERID), 'unknown')
  ),
  UPPER(
    concat(
      nvl(
        nvl(Final_Lookup_canada.state, pos.state),
        'unknown'
      ),
      '-',
      nvl(nvl(primary_city, END_USER_CITY), 'unknown'),
      '-',
      nvl(
        nvl(
          CASE
            WHEN pos.SUBREGION = 'US' THEN SUBSTR(regexp_replace(pos.postalcode, ' ', ''), 1, 5)
            ELSE SUBSTR(regexp_replace(pos.postalcode, ' ', ''), 1, 6)
          END,
          Final_Lookup_canada.Postalcode
        ),
        'unknown'
      )
    )
  ),
  product_qv._id,
  pos._ID
""")
main_pos_ebs.createOrReplaceTempView('main_pos_ebs')

# COMMAND ----------

main_pos_dis=spark.sql("""
select
  pos._ID,
  'POS' SOURCE,
  ACCOUNT_ID,
  DISTRIBUTORID,
  product_qv._id ITEM_ID,
  prod.productcode ITEMID,
  TRANDATE,
  date_format(TRANDATE, 'yyyyMMdd') TRANS_DT_ID,
  sum(QTY) QTY_IN_CASES,
  SUM(QTY * PROD.ansStdUomConv) QTY_ANS_STD_UOM_CONV,
  SUM(TRANAMT) TRANAMT,
  concat(
    nvl(
      case
        when nvl(lower(TERRITORY), 'unknown') = 'unknown' then 'unknown'
        else TERRITORY
      end,
      'unknown'
    ),
    '-',
    nvl(SALESREGION, 'unknown'),
    '-',
    nvl(lower(USERID), 'unknown')
  ) TERRITORY_ID,
  concat(
    nvl(SECONDORG_TERRITORY, 'unknown'),
    '-',
    nvl(SECONDORG_SALESREGION, 'unknown'),
    '-',
    nvl(lower(SECONDORG_USERID), 'unknown')
  ) SECONDORG_TERRITORY_ID,
  UPPER(
    concat(
      nvl(
        nvl(Final_Lookup_canada.state, pos.state),
        'unknown'
      ),
      '-',
      nvl(nvl(primary_city, END_USER_CITY), 'unknown'),
      '-',
      nvl(
        nvl(
          CASE
            WHEN pos.SUBREGION = 'US' THEN SUBSTR(regexp_replace(pos.postalcode, ' ', ''), 1, 5)
            ELSE SUBSTR(regexp_replace(pos.postalcode, ' ', ''), 1, 6)
          END,
          Final_Lookup_canada.Postalcode
        ),
        'unknown'
      )
    )
  ) LOCATION_ID,
  POSSOURCE,
  case
    when DISTRIBUTORID like '% - OTD' then 'OTD'
    when DISTRIBUTORID like '% - ITD' then 'ITD'
  end ITD_OTD_FLAG
from
  wc_qv_pos_dis_a pos
  left join s_core.product_ebs prod on prod.productcode = pos.sku
  left join (
    select
      distinct *
    from
      amazusftp1.Final_Lookup_canada
  ) Final_Lookup_canada ON CASE
    WHEN pos.SUBREGION = 'US' THEN SUBSTR(regexp_replace(pos.postalcode, ' ', ''), 1, 5)
    ELSE SUBSTR(regexp_replace(pos.postalcode, ' ', ''), 1, 6)
  END = regexp_replace(Final_Lookup_canada.Postalcode, ' ', '')
  left join s_core.product_qv on prod.productcode = product_qv.itemid
where
  pos._deleted = 'false'
GROUP BY
  ACCOUNT_ID,
  DISTRIBUTORID,
  prod.PRODUCTCODE,
  TRANDATE,
  date_format(TRANDATE, 'yyyyMMdd'),
  concat(
    nvl(
      case
        when nvl(lower(TERRITORY), 'unknown') = 'unknown' then 'unknown'
        else TERRITORY
      end,
      'unknown'
    ),
    '-',
    nvl(SALESREGION, 'unknown'),
    '-',
    nvl(lower(USERID), 'unknown')
  ),
  concat(
    nvl(SECONDORG_TERRITORY, 'unknown'),
    '-',
    nvl(SECONDORG_SALESREGION, 'unknown'),
    '-',
    nvl(lower(SECONDORG_USERID), 'unknown')
  ),
  UPPER(
    concat(
      nvl(
        nvl(Final_Lookup_canada.state, pos.state),
        'unknown'
      ),
      '-',
      nvl(nvl(primary_city, END_USER_CITY), 'unknown'),
      '-',
      nvl(
        nvl(
          CASE
            WHEN pos.SUBREGION = 'US' THEN SUBSTR(regexp_replace(pos.postalcode, ' ', ''), 1, 5)
            ELSE SUBSTR(regexp_replace(pos.postalcode, ' ', ''), 1, 6)
          END,
          Final_Lookup_canada.Postalcode
        ),
        'unknown'
      )
    )
  ),
  POSSOURCE,
  product_qv._id,
  pos._ID
""")
main_pos_dis.createOrReplaceTempView('main_pos_dis')

# COMMAND ----------

main_pos_manadj=spark.sql("""
select
  pos._ID,
  'POS' SOURCE,
  ACCOUNT_ID,
  DISTRIBUTORID,
  product_qv._id ITEM_ID,
  prod.productcode ITEMID,
  TRANDATE,
  date_format(TRANDATE, 'yyyyMMdd') TRANS_DT_ID,
  sum(QTY) QTY,
  SUM(QTY * PROD.ansStdUomConv) QTY_ANS_STD_UOM_CONV,
  SUM(TRANAMT) TRANAMT,
  concat(
    nvl(
      case
        when nvl(lower(TERRITORY), 'unknown') = 'unknown' then 'unknown'
        else TERRITORY
      end,
      'unknown'
    ),
    '-',
    nvl(SALESREGION, 'unknown'),
    '-',
    nvl(lower(USERID), 'unknown')
  ) TERRITORY_ID,
  concat(
    nvl(SECONDORG_TERRITORY, 'unknown'),
    '-',
    nvl(SECONDORG_SALESREGION, 'unknown'),
    '-',
    nvl(lower(SECONDORG_USERID), 'unknown')
  ) SECONDORG_TERRITORY_ID,
  UPPER(
    concat(
      nvl(
        nvl(Final_Lookup_canada.state, pos.state),
        'unknown'
      ),
      '-',
      nvl(nvl(primary_city, END_USER_CITY), 'unknown'),
      '-',
      nvl(
        nvl(
          CASE
            WHEN pos.SUBREGION = 'US' THEN SUBSTR(regexp_replace(pos.postalcode, ' ', ''), 1, 5)
            ELSE SUBSTR(regexp_replace(pos.postalcode, ' ', ''), 1, 6)
          END,
          Final_Lookup_canada.Postalcode
        ),
        'unknown'
      )
    )
  ) LOCATION_ID,
  POSSOURCE,
  case
    when DISTRIBUTORID like '% - OTD' then 'OTD'
    when DISTRIBUTORID like '% - ITD' then 'ITD'
  end ITD_OTD_FLAG
from
  wc_qv_pos_manadj_a pos
  left join s_core.product_ebs prod on prod.productcode = pos.sku
  left join (
    select
      distinct *
    from
      amazusftp1.Final_Lookup_canada
  ) Final_Lookup_canada ON CASE
    WHEN pos.SUBREGION = 'US' THEN SUBSTR(regexp_replace(pos.postalcode, ' ', ''), 1, 5)
    ELSE SUBSTR(regexp_replace(pos.postalcode, ' ', ''), 1, 6)
  END = regexp_replace(Final_Lookup_canada.Postalcode, ' ', '')
  left join s_core.product_qv on prod.productcode = product_qv.itemid
where
  pos._deleted = 'false'
GROUP BY
  ACCOUNT_ID,
  DISTRIBUTORID,
  prod.PRODUCTCODE,
  TRANDATE,
  date_format(TRANDATE, 'yyyyMMdd'),
  concat(
    nvl(
      case
        when nvl(lower(TERRITORY), 'unknown') = 'unknown' then 'unknown'
        else TERRITORY
      end,
      'unknown'
    ),
    '-',
    nvl(SALESREGION, 'unknown'),
    '-',
    nvl(lower(USERID), 'unknown')
  ),
  concat(
    nvl(SECONDORG_TERRITORY, 'unknown'),
    '-',
    nvl(SECONDORG_SALESREGION, 'unknown'),
    '-',
    nvl(lower(SECONDORG_USERID), 'unknown')
  ),
  UPPER(
    concat(
      nvl(
        nvl(Final_Lookup_canada.state, pos.state),
        'unknown'
      ),
      '-',
      nvl(nvl(primary_city, END_USER_CITY), 'unknown'),
      '-',
      nvl(
        nvl(
          CASE
            WHEN pos.SUBREGION = 'US' THEN SUBSTR(regexp_replace(pos.postalcode, ' ', ''), 1, 5)
            ELSE SUBSTR(regexp_replace(pos.postalcode, ' ', ''), 1, 6)
          END,
          Final_Lookup_canada.Postalcode
        ),
        'unknown'
      )
    )
  ),
  POSSOURCE,
  product_qv._id,
  pos._ID
""")
main_pos_manadj.createOrReplaceTempView('main_pos_manadj')

# COMMAND ----------

main_df = (
  main_pos_ebs
  .union(main_pos_dis)
  .union(main_pos_manadj)
)
main_df.cache()

# COMMAND ----------

# TRANSFORM DATA
main_f = (
   main_df
   .transform(attach_partition_column("TRANDATE"))
   .transform(attach_modified_date())
   .transform(attach_deleted_flag())
   .transform(apply_schema(schema))
)

if incremental:
  main_f.display()

# COMMAND ----------

# VALIDATE DATA
check_distinct_count(main_f, '_ID')

# COMMAND ----------

# LOAD
options = {'overwrite': overwrite, 'partition_column': '_PART'}
register_hive_table(main_f, target_table, target_folder, options = options)
merge_into_table(main_f, target_table, key_columns, options = {'auto_merge': True})

# COMMAND ----------

# HANDLE DELETE
full_pos_keys = spark.sql("""
  SELECT _id from g_na_tm.wc_qv_pos_a 
  where  wc_qv_pos_a._deleted = 'false'
 
 
  
""")

full_dis_keys = spark.sql("""
  SELECT _id from g_na_tm.wc_qv_pos_dis_a 
  where  wc_qv_pos_dis_a._deleted = 'false'
  

""")

full_adj_keys = spark.sql("""
  SELECT _id from g_na_tm.wc_qv_pos_manadj_a 
  where  wc_qv_pos_manadj_a._deleted = 'false'

""")

full_keys_f = (
  full_pos_keys
  .union(full_dis_keys)
  .union(full_adj_keys)
  .select('_ID')
)

apply_soft_delete(full_keys_f, table_name, key_columns = '_ID')

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not sampling:
  cutoff_value = get_max_value(wc_qv_pos_a, '_MODIFIED')
  update_cutoff_value(cutoff_value, target_table, 'g_na_tm.wc_qv_pos_a')
  update_run_datetime(run_datetime, target_table, 'g_na_tm.wc_qv_pos_a')

  cutoff_value = get_max_value(wc_qv_pos_dis_a, '_MODIFIED')
  update_cutoff_value(cutoff_value, target_table, 'g_na_tm.wc_qv_pos_dis_a')
  update_run_datetime(run_datetime, target_table, 'g_na_tm.wc_qv_pos_dis_a')

  cutoff_value = get_max_value(wc_qv_pos_manadj_a, '_MODIFIED')
  update_cutoff_value(cutoff_value, target_table, 'g_na_tm.wc_qv_pos_manadj_a')
  update_run_datetime(run_datetime, target_table, 'g_na_tm.wc_qv_pos_manadj_a')
