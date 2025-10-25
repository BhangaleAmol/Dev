import pytest
from databricks.functions.shared.tests import *
from databricks.functions.shared.transformations import (
    cast_data_types_from_schema,
    drop_key_columns,
    replace_null_string_with_null
)
from databricks.functions.shared.schema import get_dict_schema_from_config


def get_datasets():
    return {
        'prms.prmsf200_mscmp100': read_test_data('/s_core/account_prms/prms.prmsf200_mscmp100.csv'),
        'prms.prmsf200_mscmz100': read_test_data('/s_core/account_prms/prms.prmsf200_mscmz100.csv')
    }


@pytest.fixture(scope='module')
def details(request):
    return request.config.getoption('--details') == 'true'


@pytest.fixture(scope='module')
def datasets():
    return get_datasets()


@pytest.fixture(scope='module')
def expected_df():
    return read_test_data('/s_core/account_prms/_expected.csv')


def test_account_prms(datasets, expected_df, mocker, details):

    mocker.patch(
        'databricks.functions.s_core.account_prms.add_account_keys',
        lambda df: df)
    mocker.patch(
        'databricks.functions.s_core.account_prms.update_map_table',
        lambda x, y, z: 0)

    from databricks.functions.s_core.account_prms import transform_data

    drop_columns = [
        '_DATE', '_MODIFIED', '_PART', 'insertedOn', 'updatedOn']

    actual_df = (
        transform_data(datasets)
        .transform(drop_key_columns)
        .drop(*drop_columns)
    )

    table_schema = get_dict_schema_from_config('s_core.account_prms')
    excepted_df = (
        expected_df
        .transform(cast_data_types_from_schema, table_schema)
        .transform(replace_null_string_with_null)
        .transform(drop_key_columns)
        .drop(*drop_columns)
    )

    assert dataframes_equal(
        actual_df, excepted_df, options={'details': details})
