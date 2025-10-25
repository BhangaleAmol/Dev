from databricks.functions.s_core.transformations import add_account_keys
from databricks.functions.shared import *
from pyspark.sql import SparkSession
import databricks.functions.shared.transformations as t
import databricks.functions.ebs as ebs
import pyspark.sql.functions as f


def read_data(params: dict) -> dict[DataFrame]:

    if params.get('incremental'):
        cutoff_value = get_cutoff_value(
            params.get('table_name'),
            'ebs.hz_cust_accounts',
            params.get('prune_days'))
        ebs_hz_cust_accounts_df = read_table(
            'ebs.hz_cust_accounts', 'LAST_UPDATE_DATE', cutoff_value)
    else:
        ebs_hz_cust_accounts_df = read_table('ebs.hz_cust_accounts')

    return {
        'ebs.hz_cust_accounts': ebs_hz_cust_accounts_df,
        'ebs.hz_parties': read_table('ebs.hz_parties')
    }


def add_natural_key(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            'accountId',
            f.regexp_replace(f.col('CUST_ACCOUNT_ID').cast(
                'int').cast('string'), ',', '')
        )
    )


def select_columns(df: DataFrame) -> DataFrame:
    return df.select(
        f.col('accountId'),
        f.current_timestamp().alias('insertedOn'),
        f.current_timestamp().alias('updatedOn'),
        f.col('ACCOUNT_NAME').alias('name'),
        f.col('ACCOUNT_NUMBER').alias('accountNumber'),
        f.col('ATTRIBUTE1').alias('gbu'),
        f.col('ATTRIBUTE10').alias('accountGroup'),
        f.col('ATTRIBUTE11').alias('vertical'),
        f.col('ATTRIBUTE13').alias('customerDivision'),
        f.col('ATTRIBUTE15').alias('region'),
        f.col('ATTRIBUTE18').alias('eCommerceFlag'),
        f.col('ATTRIBUTE19').alias('customerTier'),
        f.col('ATTRIBUTE2').alias('industry'),
        f.col('ATTRIBUTE20').alias('customerSegmentation'),
        f.col('ATTRIBUTE3').alias('subIndustry'),
        f.col('ATTRIBUTE4').alias('accountType'),
        f.col('ATTRIBUTE8').alias('forecastGroup'),
        f.col('CREATED_BY').alias('createdBy'),
        f.col('CREATION_DATE').alias('createdOn'),
        f.col('CUSTOMER_TYPE').alias('customerType'),
        f.col('LAST_UPDATED_BY').alias('modifiedBy'),
        f.col('LAST_UPDATE_DATE').alias('modifiedOn'),
        f.col('PARTY_ID').alias('partyId'),
        f.col('PRICE_LIST_ID').alias('priceListId'),
        f.col('STATUS').alias('status'),
        f.lit(True).alias('isActive'),
        f.lit(False).alias('isDeleted'),
        f.lit('001').alias('client')
    )


def join_hz_parties(df: DataFrame, hz_parties_df: DataFrame) -> DataFrame:

    hz_parties_df = hz_parties_df.select(
        f.col('PARTY_ID').alias('partyId'),
        f.col('PARTY_NUMBER').alias('partyNumber'),
    )

    return (
        df.alias('df')
        .join(
            hz_parties_df.alias('p'),
            f.col('df.partyId') == f.col('p.partyId'),
            "left"
        )
        .select('df.*', 'p.partyNumber')
    )


def fill_missing_values(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn(
            'customerType',
            f.when(f.col('customerType') == 'I', 'Internal')
            .otherwise('External')
        )
        .withColumn(
            'forecastGroup',
            f.when(f.col('forecastGroup').isNull(), 'Other')
            .otherwise(f.col('forecastGroup'))
        )
        .withColumn(
            'eCommerceFlag',
            f.when(f.col('eCommerceFlag') == 'YES', True)
            .otherwise(False)
        )
        .withColumn(
            'registrationId',
            f.when(
                f.col('accountType').isNotNull()
                & f.col('priceListId').isNotNull()
                & (f.col('status') == 'A'),
                f.col('partyNumber')
            )
            .otherwise(None)
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
        read_table('ebs.hz_cust_accounts',)
        .transform(add_natural_key)
        .transform(t.add_source_column, 'EBS')
        .transform(add_key, ['accountId', '_SOURCE'], '_ID', 'edm.account')
        .select('_ID')
        .transform(t.attach_record_with_zero)
    )
    apply_soft_delete(full_keys_df, params.get('table_name'))


def transform_data(datasets: dict[DataFrame]) -> DataFrame:

    table_schema = get_dict_schema_from_config('s_core.account_ebs')
    table_columns = list(table_schema.keys())

    return (
        datasets['ebs.hz_cust_accounts']
        .transform(add_natural_key)
        .drop('CUST_ACCOUNT_ID')
        .transform(select_columns)
        .transform(join_hz_parties, datasets['ebs.hz_parties'])
        .transform(t.fix_data_values)
        .transform(
            t.remove_duplicates,
            ['accountId'],
            lambda x, y: send_mail_duplicate_records_found(x, y, config))
        .transform(t.add_internal_columns, source_name='EBS')
        .transform(
            ebs.fix_natural_key,
            ['accountNumber', 'createdBy', 'modifiedBy', 'partyId', 'priceListId'])
        .transform(fill_missing_values)
        .drop('partyNumber', 'status')
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
        datasets['ebs.hz_cust_accounts'], 'LAST_UPDATE_DATE')
    update_cutoff_value(
        cutoff_value,
        params.get('table_name'),
        'ebs.hz_cust_accounts')
    update_run_datetime(
        params.get('run_datetime'),
        params.get('table_name'),
        'ebs.hz_cust_accounts')
