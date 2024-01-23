#%%

import pandas as pd
from time import time
from sqlalchemy import create_engine

input_filename = 'yellow_tripdata_2021-01.csv'
chunk_size = 100000
date_columns = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']
table_name = 'yellow_taxi_data'

engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')

df_iter = pd.read_csv(input_filename,
                      iterator=True,
                      chunksize=chunk_size,
                      parse_dates=date_columns)

total_time = []
total_rows = []
for chunk, df in enumerate(df_iter):

    t_start = time()

    if df.index[0] == 0: # check if it's the first chunk
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    
    df.to_sql(name=table_name, con=engine, if_exists='append')

    t_end = time()

    total_time.append(t_end - t_start)
    total_rows.append(len(df))

    print(f'chunk {chunk + 1} was inserted | {total_time[-1]:.3f} second | {total_rows[-1]} rows')

print(f'\ntotal rows inserted = {sum(total_rows)}')
print(f'total time = {round(sum(total_time))} second')

# validation
query = """
SELECT COUNT(1) FROM yellow_taxi_data
"""
total_rows_database = pd.read_sql(query, con=engine).iloc[0, 0]
print(f'\ntotal rows in database = {total_rows_database}')
