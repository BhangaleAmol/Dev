from databricks.functions.s_core.transformations import add_account_keys
from databricks.functions.shared import *
import databricks.functions.shared.transformations as t
import pyspark.sql.functions as f


def read_data(params: dict) -> dict[DataFrame]:
    return {
        'tot.tb_pbi_customer': read_table('tot.tb_pbi_customer'),
        'tot.tb_pbi_orders': read_table('tot.tb_pbi_orders')
    }


def add_natural_key(df: DataFrame) -> DataFrame:
    return (
        df.withColumn('accountId', f.concat(
            f.col('Company'), f.lit('-'), f.col('Customer_id')))
    )


def join_orders_data(df: DataFrame, tot_tb_pbi_orders_df: DataFrame) -> DataFrame:

    tot_tb_pbi_orders_df = (
        tot_tb_pbi_orders_df
        .groupBy('Customer_id', 'Company')
        .agg(f.max('Order_included_date').alias('Order_included_date'))
        .select('Customer_id', 'Company', 'Order_included_date')
    )

    return (
        df.alias('df')
        .join(
            tot_tb_pbi_orders_df.alias('o'),
            (f.col('df.Customer_id') == f.col('o.Customer_id'))
            & (f.col('df.Company') == f.col('o.Company')),
            "left"
        )
        .select(
            'df.*',
            f.col('o.Order_included_date').alias('createdOn')
        )
    )


def fill_missing_values(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn(
            'businessGroupId',
            f.when(
                f.col('Customer_business_group') == '',
                f.lit(None)
            )
            .otherwise(
                f.concat(
                    f.lit('BusinessGroup'),
                    f.lit('-'),
                    f.col('Company'),
                    f.lit('-'),
                    f.col('Customer_business_group')
                )
            )
        )
        .withColumn(
            'gbu',
            f.when(f.col('Customer_Division') == 'I', f.lit('INDUSTRIAL'))
            .when(f.col('Customer_Division') == 'M', f.lit('MEDICAL'))
            .otherwise(f.col('Customer_Division'))
        )
    )


def select_columns(df: DataFrame) -> DataFrame:
    return df.select(
        f.coalesce(f.col('Country'), f.lit('Brazil')).alias('address1Country'),
        f.col('accountId'),
        f.col('businessGroupId'),
        f.col('createdOn'),
        f.col('customer_address').alias('address1Line1'),
        f.col('customer_city').alias('address1City'),
        f.col('Customer_Division').alias('customerDivision'),
        f.col('customer_forecast_code').alias('forecastGroup'),
        f.col('Customer_id').alias('accountNumber'),
        f.col('Customer_name').alias('name'),
        f.col('customer_state').alias('address1State'),
        f.col('Customer_TSM_cod').alias('salesOrganizationID'),
        f.col('Customer_TSM_cod').alias('territoryId'),
        f.col('gbu'),
        f.col('Tier').alias('customerTier'),
        f.current_timestamp().alias('insertedOn'),
        f.current_timestamp().alias('updatedOn'),
        f.lit('001').alias('client'),
        f.lit('External').alias('customerType'),
        f.lit(False).alias('eCommerceFlag'),
        f.lit(False).alias('isDeleted'),
        f.lit(True).alias('isActive'),
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
        read_table('tot.tb_pbi_customer', filter_deleted=True)
        .transform(extract_distinct_customers)
        .transform(add_natural_key)
        .transform(t.add_source_column, 'TOT')
        .transform(add_key, ['accountId', '_SOURCE'], '_ID', 'edm.account')
        .select('_ID')
        .transform(t.attach_record_with_zero)
    )
    apply_soft_delete(full_keys_df, params.get('table_name'))


def extract_distinct_customers(df: DataFrame) -> DataFrame:
    return (
        df
        .select(
            'Company',
            'Country',
            'customer_address',
            'Customer_business_group',
            'customer_city',
            'Customer_Division',
            'customer_forecast_code',
            'Customer_id',
            'Customer_name',
            'customer_state',
            'Customer_TSM_cod',
            'Tier'
        )
        .distinct()
    )


def transform_data(datasets: dict[DataFrame]) -> DataFrame:

    table_schema = get_dict_schema_from_config('s_core.account_tot')
    table_columns = list(table_schema.keys())

    return (
        datasets['tot.tb_pbi_customer']
        .transform(extract_distinct_customers)
        .transform(add_natural_key)
        .transform(join_orders_data, datasets['tot.tb_pbi_orders'])
        .transform(fill_missing_values)
        .transform(select_columns)
        .transform(t.fix_data_values)
        .transform(
            t.remove_duplicates,
            ['accountId'])
        # .transform(
        #     t.remove_duplicates,
        #     ['accountId'],
        #     lambda x, y: send_mail_duplicate_records_found(x, y, config))
        .transform(t.add_internal_columns, source_name='TOT')
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
