from databricks.functions.s_core.transformations import add_account_keys
from databricks.functions.shared import *
import databricks.functions.shared.transformations as t
import pyspark.sql.functions as f


def read_data(params: dict) -> dict[DataFrame]:

    if params.get('incremental'):
        cutoff_value = get_cutoff_value(
            params.get('table_name'),
            'sapp01.kna1',
            params.get('prune_days'))
        sapp01_kna1_df = read_table(
            'sapp01.kna1', '_MODIFIED', cutoff_value)
    else:
        sapp01_kna1_df = read_table('sapp01.kna1')

    return {
        'sapp01.kna1': sapp01_kna1_df
    }


def add_natural_key(df: DataFrame) -> DataFrame:
    return (
        df.withColumn('accountId', f.col('KUNNR'))
    )


def select_columns(df: DataFrame) -> DataFrame:
    return df.select(
        f.current_timestamp().alias('insertedOn'),
        f.current_timestamp().alias('updatedOn'),
        f.lit('1900-01-01 00:00:00').alias('modifiedOn'),
        f.col('accountId'),
        f.col('ERNAM').alias('createdBy'),
        f.col('ERDAT').alias('createdOn'),
        f.col('KTOKD').alias('accountGroup'),
        f.col('KUNNR').alias('accountNumber'),
        f.col('KUKLA').alias('accountType'),
        f.col('STRAS').alias('address1Line1'),
        f.col('ORT01').alias('address1City'),
        f.col('LAND1').alias('address1Country'),
        f.col('PSTLZ').alias('address1PostalCode'),
        f.col('REGIO').alias('address1State'),
        f.col('J_1KFTBUS').alias('businessType'),
        f.col('MANDT').alias('client'),
        f.lit('Other').alias('forecastGroup'),  # forecastGroup tbd
        f.col('KUKLA').alias('customerType'),
        f.col('BRSCH').alias('industry'),
        f.concat(
            f.trim(f.col('NAME1')),
            f.lit(' '),
            f.trim(f.col('NAME2')),
            f.lit(' '),
            f.trim(f.col('NAME3')),
            f.lit(' '),
            f.trim(f.col('NAME4')),
        ).alias('name'),
        f.col('REGIO').alias('region'),
    )


def save_data(df: DataFrame, params: dict):
    create_database(params.get('database_name'))

    options = {'partition_columns': params.get('partition_columns')}
    create_table(
        params.get('table_name'),
        df.schema,
        params.get('table_path'), options)

    options = {
        'append_only': params.get('append_only'),
        'incremental': params.get('incremental'),
        'key_columns': '_ID',
        'overwrite': params.get('overwrite'),
        'partition_columns': params.get('partition_columns')
    }
    save_to_table(df, params.get('table_name'), options)


def soft_delete(params: dict):
    full_keys_df = (
        read_table('sapp01.kna1')
        .transform(add_natural_key)
        .transform(t.add_source_column, 'SAP')
        .transform(add_key, ['accountId', '_SOURCE'], '_ID', 'edm.account')
        .select('_ID')
        .transform(t.attach_record_with_zero)
    )
    apply_soft_delete(full_keys_df, params.get('table_name'))


def transform_data(datasets: dict[DataFrame]) -> DataFrame:

    table_schema = get_dict_schema_from_config('s_core.account_sap')
    table_columns = list(table_schema.keys())

    return (
        datasets['sapp01.kna1']
        .transform(add_natural_key)
        .transform(select_columns)
        .transform(t.fix_data_values)
        .filter(f.length(f.trim(f.col('accountNumber'))) != 0)
        .transform(
            t.remove_duplicates,
            ['accountId'],
            lambda x, y: send_mail_duplicate_records_found(x, y, config))
        .transform(t.add_internal_columns, source_name='SAP')
        .transform(t.add_missing_columns, table_columns)
        .withColumn('_PART', f.lit('1900-01-01'))  # field to be deleted
        .execute(update_map_table, 'edm.account', ['accountId', '_SOURCE'])
        .transform(add_account_keys)
        .transform(t.cast_data_types_from_schema, table_schema)
        .select(table_columns)
        .transform(t.attach_unknown_record)
        .transform(t.sort_internal_columns_first)
    )


def update_keys(params: dict):
    update_key(
        params.get('table_name'), ['createdBy', '_SOURCE'], 'createdBy_ID', 'edm.user')
    update_key(
        params.get('table_name'), ['modifiedBy', '_SOURCE'], 'modifiedBy_ID', 'edm.user')


def update_timestamps(datasets: dict[DataFrame], params: dict):
    cutoff_value = get_max_value(
        datasets['sapp01.kna1'], '_MODIFIED')
    update_cutoff_value(
        cutoff_value,
        params.get('table_name'),
        'sapp01.kna1')
    update_run_datetime(
        params.get('run_datetime'),
        params.get('table_name'),
        'sapp01.kna1')
