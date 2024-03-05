#%%
import pyspark
from pyspark.sql import SparkSession
print(pyspark.__version__)

#%%
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

#%%
df_green = spark.read.parquet('data/pq/green/*/*')

# """
# SELECT
#     -- Revenue grouping
#     date_trunc('hour', lpep_pickup_datetime) AS hour,
#     PULocationID AS zone,

#     -- Revenue calculation
#     SUM(total_amount) AS amount,
#     COUNT(1) AS number_records
# FROM
#     green
# WHERE
#     lpep_pickup_datetime >= '2020-01-01 00:00:00'
# GROUP BY
#     1, 2
# """
# %%
rdd = df_green \
    .select('lpep_pickup_datetime', 'PULocationID', 'total_amount') \
    .rdd
# %%
from datetime import datetime
start = datetime(year=2021, month=1, day=1)

def filter_outliers(row):
    return row.lpep_pickup_datetime >= start

#%%
def prepare_for_grouping(row):
    hour = row.lpep_pickup_datetime.replace(minute=0, second=0, microsecond=0)
    zone = row.PULocationID
    key = (hour, zone)
    
    amount = row.total_amount
    count = 1
    value = (amount, count)

    return (key, value)
#%%
def calculate_revenue(left_value, right_value):
    left_amount, left_count = left_value
    right_amount, right_count = right_value

    output_amount = left_amount + right_amount
    output_count = left_count + right_count

    return (output_amount, output_count)
#%%
from collections import namedtuple
RevenueRow = namedtuple('RevenueRow', ['hour', 'zone', 'revenue', 'count'])

def unwrap(row):
    return RevenueRow(
        hour=row[0][0],
        zone=row[0][1],
        revenue=row[1][0],
        count=row[1][1]
    )
#%%
from pyspark.sql import types

result_schema = types.StructType([
    types.StructField('hour', types.TimestampType(), True), 
    types.StructField('zone', types.IntegerType(), True), 
    types.StructField('revenue', types.DoubleType(), True), 
    types.StructField('count', types.IntegerType(), True)
])

df_result = rdd \
    .filter(filter_outliers) \
    .map(prepare_for_grouping) \
    .reduceByKey(calculate_revenue) \
    .map(unwrap) \
    .toDF(result_schema)

# %%
df_result.write.parquet('data/tmp/green-revenue/')
# %%
# mapPartition
columns = ['VendorID', 'lpep_pickup_datetime', 'PULocationID', 'DOLocationID', 'trip_distance']

duration_rdd = df_green \
    .select(columns) \
    .rdd
#%%
import pandas as pd
rows = duration_rdd.take(10)

# model = ...
def model_precict(df):
    # y_pred = model.predict(df)
    y_pred = df.trip_distance * 5
    return y_pred
#%%
def infinite_seq():
    i = 0
    while True:
        yield i
        i = i + 1
seq = infinite_seq()
#%%
next(seq)
#%%
def apply_model_in_batch(rows):
    df = pd.DataFrame(rows, columns=columns)
    predictions = model_precict(df)
    df['predicted_duration'] = predictions

    for row in df.itertuples():
        yield row

df_predicts = duration_rdd \
    .mapPartitions(apply_model_in_batch) \
    .toDF() \
    .drop('Index') \
    .select('predicted_duration') \
    .show()
# %%
