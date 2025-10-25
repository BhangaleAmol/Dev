import pandas as pd

file_paths = [
    "./tests/notebooks/test_data/s_core/account_col/_expected.csv",
    "./tests/notebooks/test_data/s_core/account_agg/s_core.account_col.csv",

    "./tests/notebooks/test_data/s_core/account_ebs/_expected.csv",
    "./tests/notebooks/test_data/s_core/account_agg/s_core.account_ebs.csv",

    "./tests/notebooks/test_data/s_core/account_kgd/_expected.csv",
    "./tests/notebooks/test_data/s_core/account_agg/s_core.account_kgd.csv",

    "./tests/notebooks/test_data/s_core/account_prms/_expected.csv",
    "./tests/notebooks/test_data/s_core/account_agg/s_core.account_prms.csv",

    "./tests/notebooks/test_data/s_core/account_sap/_expected.csv",
    "./tests/notebooks/test_data/s_core/account_agg/s_core.account_sap.csv",

    "./tests/notebooks/test_data/s_core/account_sf/_expected.csv",
    "./tests/notebooks/test_data/s_core/account_agg/s_core.account_sf.csv",

    "./tests/notebooks/test_data/s_core/account_tot/_expected.csv",
    "./tests/notebooks/test_data/s_core/account_agg/s_core.account_tot.csv",
]

for file_path in file_paths:
    print(file_path)
    df = pd.read_csv(file_path, keep_default_na=False, dtype=str)
    if '_DATE' not in df:
        df.insert(
            df.columns.get_loc('territory_ID') + 1, '_DATE', '1900-01-01')
    # print(df.head())

    df.to_csv(file_path, index=False)
