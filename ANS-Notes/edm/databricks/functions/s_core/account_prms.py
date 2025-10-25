from databricks.functions.s_core.transformations import add_account_keys
from databricks.functions.shared import *
import databricks.functions.shared.transformations as t
import pyspark.sql.functions as f
import databricks.functions.prms as prms


def read_data(params: dict) -> dict[DataFrame]:
    return {
        'prms.prmsf200_mscmp100': read_table('prms.prmsf200_mscmp100'),
        'prms.prmsf200_mscmz100': read_table('prms.prmsf200_mscmz100')
    }


def add_natural_key(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            'accountId',
            f.concat(
                f.regexp_replace(f.col('CMPNO').cast(
                    'int').cast('string'), ',', ''),
                f.lit('-'),
                f.regexp_replace(f.col('CUSNO').cast(
                    'int').cast('string'), ',', '')
            )
        )
    )


def select_columns(df: DataFrame) -> DataFrame:
    return df.select(
        f.current_timestamp().alias('insertedOn'),
        f.current_timestamp().alias('updatedOn'),
        f.col('accountId'),
        f.col('USRIDA').alias('createdBy'),
        f.col('ADDDT').alias('createdOn'),
        f.col('USRIDA').alias('modifiedBy'),
        f.col('ADDDT').alias('modifiedOn'),
        f.col('CUSNO').alias('accountNumber'),
        f.col('CADD1').alias('address1Line1'),
        f.col('CADD2').alias('address1Line2'),
        f.concat(
            f.col('CADDX'),
            f.lit(' '),
            f.col('CADD3')
        ).alias('address1Line3'),
        f.col('CZIPC').alias('address1PostalCode'),
        f.col('CSTTE').alias('address1State'),
        f.lit('001').alias('client'),
        f.col('CADIV').alias('customerDivision'),
        f.col('ICCFLG').alias('customerType'),
        f.lit('Other').alias('forecastGroup'),
        f.lit(True).alias('isActive'),
        f.lit(False).alias('isDeleted'),
        f.col('CNAME').alias('name'),
    )


def fill_missing_values(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn(
            'customerType',
            f.when(f.trim(f.col('customerType')) == 'Y', 'Internal')
            .otherwise('External')
        )
    )


def join_customer_data(df: DataFrame, prms_prmsf200_mscmz100_df: DataFrame) -> DataFrame:
    return (
        df.alias('df')
        .join(
            prms_prmsf200_mscmz100_df.alias('c'),
            (f.col('df.CUSNO') == f.col('c.CUSNO'))
            & (f.col('df.CMPNO') == f.col('c.CMPNO')),
            "left"
        )
        .select('df.*', 'c.USRIDA', 'c.ADDDT', 'c.USRIDA', 'c.CADIV', 'c.ICCFLG')
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
        read_table('prms.prmsf200_mscmp100', filter_deleted=True)
        .transform(add_natural_key)
        .transform(t.add_source_column, 'PRMS')
        .transform(add_key, ['accountId', '_SOURCE'], '_ID', 'edm.account')
        .select('_ID')
        .transform(t.attach_record_with_zero)
    )
    apply_soft_delete(full_keys_df, params.get('table_name'))


def transform_data(datasets: dict[DataFrame]) -> DataFrame:

    table_schema = get_dict_schema_from_config('s_core.account_ebs')
    table_columns = list(table_schema.keys())

    return (
        datasets['prms.prmsf200_mscmp100']
        .transform(join_customer_data, datasets['prms.prmsf200_mscmz100'])
        .transform(add_natural_key)
        .transform(select_columns)
        .transform(prms.fix_dates, ['createdOn', 'modifiedOn'])
        .transform(t.fix_data_values)
        .transform(
            t.remove_duplicates,
            ['accountId'],
            lambda x, y: send_mail_duplicate_records_found(x, y, config))
        .transform(t.add_internal_columns, source_name='PRMS')
        .transform(prms.fix_natural_key, ['accountNumber'])
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
