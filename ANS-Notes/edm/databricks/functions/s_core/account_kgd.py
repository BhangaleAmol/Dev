from databricks.functions.s_core.transformations import add_account_keys
from databricks.functions.shared import *
import databricks.functions.shared.transformations as t
import pyspark.sql.functions as f
import databricks.functions.kgd as kgd


def read_data(params: dict) -> dict[DataFrame]:

    if params.get('incremental'):
        cutoff_value = get_cutoff_value(
            params.get('table_name'),
            'kgd.dbo_tembo_customer',
            params.get('prune_days'))
        kgd_dbo_tembo_customer_df = read_table(
            'kgd.dbo_tembo_customer', 'LastModifyDate', cutoff_value, filter_deleted=True)
    else:
        kgd_dbo_tembo_customer_df = read_table(
            'kgd.dbo_tembo_customer', filter_deleted=True)

    return {
        'kgd.dbo_tembo_customer': kgd_dbo_tembo_customer_df
    }


def add_natural_key(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            'accountId',
            f.regexp_replace(f.col('Cust_ID').cast(
                'int').cast('string'), ',', '')
        )
        .drop('Cust_ID')
    )


def select_columns(df: DataFrame) -> DataFrame:
    return df.select(
        f.current_timestamp().alias('insertedOn'),
        f.current_timestamp().alias('updatedOn'),
        f.col('accountId'),
        f.col('AddressCountry').alias('address1Country'),
        f.col('CreateBy').alias('createdBy'),
        f.col('CreateDate').alias('createdOn'),
        f.col('accountId').alias('accountNumber'),
        f.col('CustomerName').alias('name'),
        f.col('CustomerType').alias('accountType'),
        f.col('ForecastCode').alias('forecastGroup'),
        f.col('GBU').alias('gbu'),
        f.col('LastModifyDate').alias('modifiedOn'),
        f.col('PostalCode').alias('address1PostalCode'),
    )


def fill_missing_values(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn(
            'forecastGroup',
            f.when(f.trim(f.col('forecastGroup')) == '', 'Other')
            .otherwise(f.trim(f.col('forecastGroup')))
        )
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
        read_table('kgd.dbo_tembo_customer', filter_deleted=True)
        .transform(add_natural_key)
        .transform(t.add_source_column, 'KGD')
        .transform(add_key, ['accountId', '_SOURCE'], '_ID', 'edm.account')
        .select('_ID')
        .transform(t.attach_record_with_zero)
    )
    apply_soft_delete(full_keys_df, params.get('table_name'))


def transform_data(datasets: dict[DataFrame]) -> DataFrame:

    table_schema = get_dict_schema_from_config('s_core.account_ebs')
    table_columns = list(table_schema.keys())

    return (
        datasets['kgd.dbo_tembo_customer']
        .transform(add_natural_key)
        .transform(select_columns)
        .transform(t.fix_data_values)
        .transform(
            t.remove_duplicates,
            ['accountId'],
            lambda x, y: send_mail_duplicate_records_found(x, y, config))
        .transform(t.add_internal_columns, source_name='KGD')
        .transform(kgd.fix_natural_key, ['accountNumber'])
        .transform(fill_missing_values)
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
        datasets['kgd.dbo_tembo_customer'], 'LastModifyDate')
    update_cutoff_value(
        cutoff_value,
        params.get('table_name'),
        'kgd.dbo_tembo_customer')
    update_run_datetime(
        params.get('run_datetime'),
        params.get('table_name'),
        'kgd.dbo_tembo_customer')
