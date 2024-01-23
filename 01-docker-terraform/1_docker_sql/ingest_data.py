import argparse
import os
import pandas as pd
from time import time
from sqlalchemy import create_engine
from urllib.parse import urlparse

def main(params):

    user = params.user
    # not safe to pass password like this, env variables is safer
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    chunk_size = params.chunk_size
    url = params.url

    parsed_url = urlparse(url)
    csv_name = os.path.basename(parsed_url.path)

    os.system(f'wget {url}')
    if csv_name.split('.')[-1] != 'csv':
        os.system(f'gzip -df {csv_name}')

    csv_name = '.'.join(csv_name.split('.')[0:2])

    header = pd.read_csv(csv_name, index_col=0, nrows=0).columns.tolist()

    # identify date columns to ensure they are handled as timestamps in db
    # this list should not be hardcoded
    date_columns = ['tpep_pickup_datetime',
                    'tpep_dropoff_datetime',
                    'lpep_pickup_datetime',
                    'lpep_dropoff_datetime']
    date_columns = list(set(header).intersection(set(date_columns)))
    df_iter = pd.read_csv(csv_name,
                          iterator=True,
                          chunksize=chunk_size,
                          parse_dates=date_columns)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    for chunk, df in enumerate(df_iter):

        t_start = time()

        if df.index[0] == 0: # check if it's the first chunk
            df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        
        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()

        print(f'chunk {chunk + 1} was inserted | {t_end - t_start:.3f} second')


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='db for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--chunk_size', type=int, help='chunk size for ingestion in batch')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()
    
    main(args)
