import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """

    year = '2020'
    months = ['10', '11', '12']

    taxi_dtypes = {
        'VendorID' : pd.Int64Dtype(),
        'passenger_count' : pd.Int64Dtype(),
        'trip_distance' : float,
        'RatecodeID' : pd.Int64Dtype(),
        'store_and_fwd_flag' : str,
        'PULocationID' : pd.Int64Dtype(),
        'DOLocationID' : pd.Int64Dtype(),
        'payment_type' : pd.Int64Dtype(),
        'fare_amount' : float,
        'extra' : float,
        'mta_tax' : float,
        'tip_amount' : float,
        'tolls_amount' : float,
        'improvement_surcharge' : float,
        'total_amount' : float,
        'congestion_surcharge' : float,
        # 'ehail_fee' : pd.Int64Dtype(),
        # 'trip_type' : pd.Int64Dtype()
    }
    
    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

    # Initialize an empty DataFrame
    df = pd.DataFrame()

    for month in months:
        url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_{year}-{month}.csv.gz'
        df_temp = pd.read_csv(url, sep=",", compression="gzip", dtype=taxi_dtypes, parse_dates=parse_dates)
        df = pd.concat([df, df_temp])
    df.reset_index(drop=True, inplace=True)
    print(df.shape)

    # df = pd.read_csv(url)
    # for i in df.columns.tolist():
    #     print(i,'\n')
    # print(df.dtypes,'\n')
    # print(df.head)
    # print(len(df.columns))
    # print(df.trip_type.unique())

    return df

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
