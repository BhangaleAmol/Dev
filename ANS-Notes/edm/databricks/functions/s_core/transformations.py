from databricks.functions.shared.dataframe import DataFrame
from databricks.functions.shared.keys import add_key, add_hash_key


def add_account_keys(df: DataFrame) -> DataFrame:
    return (
        df
        .na.drop(subset=['accountId', '_SOURCE'])
        .transform(add_key, ['accountId', '_SOURCE'], '_ID', 'edm.account')
        .transform(add_key, ['createdBy', '_SOURCE'], 'createdBy_ID', 'edm.user')
        .transform(add_key, ['modifiedBy', '_SOURCE'], 'modifiedBy_ID', 'edm.user')
        .transform(add_hash_key, ['registrationId', '_SOURCE'], "registration_ID")
        .transform(add_key, ['salesOrganizationID', '_SOURCE'], 'salesOrganization_ID', 'edm.salesorganization')
        .transform(add_hash_key, ['territoryId', '_SOURCE'], 'territory_ID')
        .transform(add_hash_key, ['businessGroupId', '_SOURCE'], 'businessGroup_ID')
        .transform(add_hash_key, ['ownerId', '_SOURCE'], 'owner_ID')
    )
