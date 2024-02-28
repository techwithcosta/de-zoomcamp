#%%

import pyspark
from pyspark.sql import SparkSession, types
print(pyspark.__version__)

import pandas as pd
import os

#%%

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

# %%

# df_green_pd = pd.read_csv('data/raw/green/2021/01/green_tripdata_2021_01.csv.gz', nrows=1000)

# spark.createDataFrame(df_green_pd).schema

# df_yellow_pd = pd.read_csv('data/raw/yellow/2021/01/yellow_tripdata_2021_01.csv.gz', nrows=1000)

# spark.createDataFrame(df_yellow_pd).schema

# %%

green_schema = types.StructType([
    types.StructField('VendorID', types.IntegerType(), True),
    types.StructField('lpep_pickup_datetime', types.TimestampType(), True),
    types.StructField('lpep_dropoff_datetime', types.TimestampType(), True),
    types.StructField('store_and_fwd_flag', types.StringType(), True),
    types.StructField('RatecodeID', types.IntegerType(), True),
    types.StructField('PULocationID', types.IntegerType(), True),
    types.StructField('DOLocationID', types.IntegerType(), True),
    types.StructField('passenger_count', types.IntegerType(), True),
    types.StructField('trip_distance', types.DoubleType(), True),
    types.StructField('fare_amount', types.DoubleType(), True),
    types.StructField('extra', types.DoubleType(), True),
    types.StructField('mta_tax', types.DoubleType(), True),
    types.StructField('tip_amount', types.DoubleType(), True),
    types.StructField('tolls_amount', types.DoubleType(), True),
    types.StructField('ehail_fee', types.DoubleType(), True),
    types.StructField('improvement_surcharge', types.DoubleType(), True),
    types.StructField('total_amount', types.DoubleType(), True),
    types.StructField('payment_type', types.IntegerType(), True),
    types.StructField('trip_type', types.IntegerType(), True),
    types.StructField('congestion_surcharge', types.DoubleType(), True)
])

yellow_schema = types.StructType([
    types.StructField('VendorID', types.IntegerType(), True),
    types.StructField('tpep_pickup_datetime', types.TimestampType(), True),
    types.StructField('tpep_dropoff_datetime', types.TimestampType(), True),
    types.StructField('passenger_count', types.IntegerType(), True),
    types.StructField('trip_distance', types.DoubleType(), True),
    types.StructField('RatecodeID', types.IntegerType(), True),
    types.StructField('store_and_fwd_flag', types.StringType(), True),
    types.StructField('PULocationID', types.IntegerType(), True),
    types.StructField('DOLocationID', types.IntegerType(), True),
    types.StructField('payment_type', types.IntegerType(), True),
    types.StructField('fare_amount', types.DoubleType(), True),
    types.StructField('extra', types.DoubleType(), True),
    types.StructField('mta_tax', types.DoubleType(), True),
    types.StructField('tip_amount', types.DoubleType(), True),
    types.StructField('tolls_amount', types.DoubleType(), True),
    types.StructField('improvement_surcharge', types.DoubleType(), True),
    types.StructField('total_amount', types.DoubleType(), True),
    types.StructField('congestion_surcharge', types.DoubleType(), True)
])

#%%

def convert_csv_to_parquet(name, schema):
    def list_paths(directory):
        """
        List all files in a directory and its subdirectories.
        """
        path_list = []
        for root, dirs, files in os.walk(directory):
            for file in files:
                path_list.append(root)
        return sorted(list(set(path_list)))

    for file in list_paths(f'data/raw/{name}'):
        input_path = file
        output_path = file.replace('/raw/', '/pq/')
        print(f'processing {output_path}')

        df = spark.read \
            .option("header", "true") \
            .schema(schema) \
            .csv(input_path)

        # because we have 4 processors, let's repartition data into 4
        df \
            .repartition(4) \
            .write.parquet(output_path, mode='overwrite')
    
    # df = spark.read.parquet(f'data/raw/{name}/*/*')

convert_csv_to_parquet('green', green_schema)
convert_csv_to_parquet('yellow', yellow_schema)
#%%
