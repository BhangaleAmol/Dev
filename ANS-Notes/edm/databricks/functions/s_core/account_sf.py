from databricks.functions.s_core.transformations import add_account_keys
from databricks.functions.shared import *
import databricks.functions.shared.transformations as t
import pyspark.sql.functions as f


def read_data(params: dict) -> dict[DataFrame]:
    return {
        'sf.account': read_table('sf.account'),
        'smartsheets.edm_control_table': read_table('smartsheets.edm_control_table', filter_deleted=True)
    }


def add_natural_key(df: DataFrame) -> DataFrame:
    return (
        df.withColumn('accountId', f.col('id'))
    )


def join_region_and_market_type(df: DataFrame, edm_control_table_df: DataFrame) -> DataFrame:

    edm_control_table_df = (
        edm_control_table_df
        .where(f.col('ACTIVE_FLG') == True)
        .where(f.col('table_id').isin('SFDC_REGIONS', 'SFDC_MARKET_TYPE'))
        .drop('rowNumber')
        .distinct()
        .select('CVALUE', 'KEY_VALUE', 'TABLE_ID')
    )

    return (
        df.alias('df')
        .join(
            edm_control_table_df.alias('gr'),
            (f.col('df.region__c') == f.col('gr.KEY_VALUE'))
            & (f.col('gr.TABLE_ID') == 'SFDC_REGIONS'),
            "left"
        )
        .join(
            edm_control_table_df.alias('mt'),
            (f.col('df.region__c') == f.col('mt.KEY_VALUE'))
            & (f.col('mt.TABLE_ID') == 'SFDC_MARKET_TYPE'),
            "left"
        )
        .select(
            'df.*',
            f.col('gr.CVALUE').alias('globalRegion'),
            f.col('mt.CVALUE').alias('marketType')
        )
    )


def select_columns(df: DataFrame) -> DataFrame:
    return df.select(
        f.current_timestamp().alias('insertedOn'),
        f.current_timestamp().alias('updatedOn'),
        f.lit('030').alias('client'),
        f.lit('Other').alias('forecastGroup'),
        f.lit('External').alias('customerType'),
        f.col('accountId'),
        f.col('BillingCity').alias('address1City'),
        f.col('BillingCountry').alias('address1Country'),
        f.col('BillingPostalCode').alias('address1PostalCode'),
        f.col('BillingState').alias('address1State'),
        f.col('BillingStreet').alias('address1Line1'),
        f.col('Business_Type__c').alias('businessType'),
        f.col('CreatedById').alias('createdBy'),
        f.col('CreatedDate').alias('createdOn'),
        f.col('Customer_Tier__c').alias('customerTier'),
        f.col('EBS_Account_Number__c').alias('ebsAccountNumber'),
        f.col('ERP_Id__c').alias('erpId'),
        f.col('globalRegion'),
        f.col('id').alias('accountNumber'),
        f.col('isdeleted').alias('isDeleted'),
        f.col('Kingdee_ID__c').alias('kingdeeId'),
        f.col('LastModifiedById').alias('modifiedBy'),
        f.col('LastModifiedDate').alias('modifiedOn'),
        f.col('Location_Type__c').alias('locationType'),
        f.col('marketType'),
        f.col('name'),
        f.col('National_Account__c').alias('nationalAccount'),
        f.col('No_of_Operating_Rooms__c').alias('numberOperatingRooms'),
        f.col('Organisation2__c').alias('organization'),
        f.col('Owner_Region__c').alias('ownerRegion'),
        f.col('OwnerId').alias('ownerId'),
        f.col('ParentId').alias('parentAccountId'),
        f.col('region__c').alias('region'),
        f.col('SAP_Sold_To_Party__c').alias('sapId'),
        f.col('status__c').alias('accountStatus'),
        f.col('Sub_Vertical__c').alias('subVertical'),
        f.col('Sync_with_Guardian__c').alias('syncWithGuardian'),
        f.col('Territory__c').alias('salesOrganizationID'),
        f.col('Territory__c').alias('territoryId'),
        f.col('Top_X_Account_Guardian__c').alias('topXAccountGuardian'),
        f.col('Top_X_Target_Guardian__c').alias('topXTargetGuardian'),
        f.col('Type__c').alias('accountType'),
        f.col('Vertical__c').alias('vertical'),
        f.expr('NOT CAST(isdeleted AS BOOLEAN)').alias('isActive'),
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
        read_table('sf.account', filter_deleted=True)
        .transform(add_natural_key)
        .transform(t.add_source_column, 'SF')
        .transform(add_key, ['accountId', '_SOURCE'], '_ID', 'edm.account')
        .select('_ID')
        .transform(t.attach_record_with_zero)
    )
    apply_soft_delete(full_keys_df, params.get('table_name'))


def transform_data(datasets: dict[DataFrame]) -> DataFrame:

    table_schema = get_dict_schema_from_config('s_core.account_sf')
    table_columns = list(table_schema.keys())

    return (
        datasets['sf.account']
        .transform(add_natural_key)
        .transform(join_region_and_market_type, datasets['smartsheets.edm_control_table'])
        .transform(select_columns)
        .transform(t.fix_data_values)
        .transform(
            t.remove_duplicates,
            ['accountId'],
            lambda x, y: send_mail_duplicate_records_found(x, y, config))
        .transform(t.add_internal_columns, source_name='SF')
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
