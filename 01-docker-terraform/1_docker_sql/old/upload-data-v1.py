#%%

import pandas as pd

pd.__version__

df = pd.read_csv('yellow_tripdata_2021-01.csv', nrows=100)

df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

from sqlalchemy import create_engine

engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')

print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))

#%%

df_iter = pd.read_csv('yellow_tripdata_2021-01.csv', iterator=True, chunksize=100000)

df = next(df_iter)

df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')

#%%

df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')

#%%

from time import time

try:
    while True:
        t_start = time()
        df = next(df_iter)
        
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')

        t_end = time()

        print('inserted another chunk, took %.3f second' % (t_end - t_start))

except StopIteration:
    print('all chunks were inserted')

# %%

query = """
SELECT *
FROM yellow_taxi_data
LIMIT 10;
"""
pd.read_sql(query, con=engine)

#%%

query = """
SELECT max(tpep_pickup_datetime), min(tpep_pickup_datetime), max(total_amount) FROM yellow_taxi_data;
"""
pd.read_sql(query, con=engine)

# minimum is year 2008, weird since df is for 2021

#%%

query = """
SELECT EXTRACT(YEAR FROM tpep_pickup_datetime) AS unique_year, COUNT(*) AS year_count
FROM yellow_taxi_data
GROUP BY unique_year
ORDER BY unique_year DESC;
"""
pd.read_sql(query, con=engine)
