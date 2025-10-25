from databricks.functions.s_core.transformations import add_account_keys
from databricks.functions.shared import *
import databricks.functions.shared.transformations as t
import pyspark.sql.functions as f


def read_data() -> dict[DataFrame]:
    return {
        'col.vw_customers': read_table('col.vw_customers'),
        'smartsheets.edm_control_table': read_table('smartsheets.edm_control_table')
    }


def select_columns(df: DataFrame) -> DataFrame:
    return df.select(
        f.current_timestamp().alias('insertedOn'),
        f.current_timestamp().alias('updatedOn'),
        f.col('Company'),
        f.col('Customer_id'),
        f.col('Customer_name').alias('name'),
        f.col('Customer_id').alias('accountNumber'),
        f.col('Country').alias('address1Country'),
        f.col('Customer_address').alias('address1Line1'),
        f.col('Customer_business_group').alias('businessGroupId'),
        f.col('Customer_city').alias('address1City'),
        f.col('Customer_postal_cod').alias('address1PostalCode'),
        f.col('Customer_state').alias('address1State'),
        f.col('Customer_type').alias('customerType'),
        f.lit('001').alias('client'),
        f.lit('I').alias('gbu'),
        f.lit(True).alias('isActive'),
        f.lit(False).alias('isDeleted'),
        f.lit('1900-01-01').alias('_PART')  # field to be deleted
    )


def fill_missing_values(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn(
            'address1Country',
            f.when(f.length(f.col('address1Country')) == 0, 'COLOMBIA')
            .otherwise(f.col('address1Country'))
        )
        .withColumn(
            'customerType',
            f.when(f.col('customerType').isNull(), 'External')
            .otherwise(f.col('customerType'))
        )
        .withColumn(
            'forecastGroup',
            f.when(f.col('forecastGroup').isNull(), f.lit('Other'))
            .otherwise(f.col('forecastGroup'))
        )
    )


def add_natural_key(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            'accountId',
            f.concat(f.col('Company'), f.lit('-'), f.col('Customer_id'))
        )
    )


def join_forecast_groups(df: DataFrame, edm_control_table_df: DataFrame) -> DataFrame:
    forecast_groups_df = (
        edm_control_table_df
        .where(f.col('table_id') == 'COLUMBIA_FORECAST_GROUPS')
        .drop('rowNumber')
        .distinct()
        .select(
            f.col('CVALUE').alias('forecastGroup'),
            f.col('KEY_VALUE').alias('Customer_id')
        )
    )

    return (
        df.alias('df')
        .join(
            forecast_groups_df.alias('fg'),
            f.col('df.Customer_id') == f.col('fg.Customer_id'),
            "left"
        )
        .select('df.*', 'fg.forecastGroup')
    )


def transform_data(datasets: dict[DataFrame]) -> DataFrame:

    table_schema = get_dict_schema_from_config('s_core.account_col')
    table_columns = list(table_schema.keys())

    return (
        datasets['col.vw_customers']
        .transform(select_columns)
        .transform(add_natural_key)
        .transform(join_forecast_groups, datasets['smartsheets.edm_control_table'])
        .drop('Company', 'Customer_id')
        .transform(t.fix_data_values)
        .transform(fill_missing_values)
        .transform(
            t.remove_duplicates,
            ['accountId'],
            lambda x, y: send_mail_duplicate_records_found(x, y, config)
        )
        .transform(t.add_internal_columns, source_name='COL')
        .transform(t.add_missing_columns, table_columns)
        .execute(update_map_table, 'edm.account', ['accountId', '_SOURCE'])
        .transform(add_account_keys)
        .transform(t.cast_data_types_from_schema, table_schema)
        .select(table_columns)
        .transform(t.attach_unknown_record)
        .transform(t.sort_internal_columns_first)
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
        'key_columns': params.get('key_columns'),
        'overwrite': params.get('overwrite'),
        'partition_columns': params.get('partition_columns')
    }
    save_to_table(df, params.get('table_name'), options)
