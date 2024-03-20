#%%

# !sudo docker stop $(sudo docker ps -q)
# !sudo docker compose up -d
# !sudo docker exec -it redpanda-1 bash

# !rpk topic create green-trips
# TOPIC        STATUS
# green-trips  OK

# if needed:
# !rpk topic delete green-trips

# !rpk topic consume green-trips
#%%

import json
import time 

from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

print(producer.bootstrap_connected())
# %%
t0 = time.time()

topic_name = 'test-topic'

# sending the messages
t0_messages = time.time()
for i in range(10):
    message = {'number': i}
    producer.send(topic_name, value=message)
    print(f"Sent: {message}")
    time.sleep(0.05)
print(f'\nmessages took {(time.time() - t0_messages):.2f} seconds')

# flushing
t0_flushing = time.time()
producer.flush()
print(f'flushing took {(time.time() - t0_flushing):.2f} seconds')

print(f'everything took {(time.time() - t0):.2f} seconds')
# %%
import pandas as pd

usecols = [
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'tip_amount'
]

# parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

# dtypes = {
#     'PULocationID' : pd.Int64Dtype(),
#     'DOLocationID' : pd.Int64Dtype(),
#     'passenger_count' : pd.Int64Dtype(),
#     'trip_distance' : float,
#     'tip_amount' : float
# }

# NOTE: can't use 'parse_dates' nor 'dtype' because those objects are not serializable

df_green = pd.read_csv('green_tripdata_2019-10.csv.gz',
                       usecols=usecols,
                    #    parse_dates=parse_dates,
                    #    dtype=dtypes,
                       sep=",",
                       compression="gzip")

df_length = len(df_green)
print(f'Sending {df_length} messages\n')

t0 = time.time()

topic_name = 'green-trips'

# sending the messages
counter = 0
for row in df_green.itertuples(index=False):
    counter += 1
    message = {col: getattr(row, col) for col in row._fields}
    producer.send(topic_name, value=message)
    print(f"Sent message {counter} / {df_length}")

print(f'\nSending took {round(time.time() - t0, 0)} seconds')

# flushing
producer.flush()
# %%
import pyspark
from pyspark.sql import SparkSession

pyspark_version = pyspark.__version__
kafka_jar_package = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark_version}"

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("GreenTripsConsumer") \
    .config("spark.jars.packages", kafka_jar_package) \
    .getOrCreate()
# %%
green_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "green-trips") \
    .option("startingOffsets", "earliest") \
    .load()
#%%
def peek(mini_batch, batch_id):
    first_row = mini_batch.take(1)

    if first_row:
        print(first_row[0])

query = green_stream.writeStream.foreachBatch(peek).start()
# Row(key=None, value=bytearray(b'{"lpep_pickup_datetime": "2019-10-01 00:26:02", "lpep_dropoff_datetime": "2019-10-01 00:39:58", "PULocationID": 112, "DOLocationID": 196, "passenger_count": 1.0, "trip_distance": 5.88, "tip_amount": 0.0}'), topic='green-trips', partition=0, offset=0, timestamp=datetime.datetime(2024, 3, 20, 11, 28, 29, 571000), timestampType=0)

# %%
query.stop()
# %%
from pyspark.sql import types

schema = types.StructType() \
    .add("lpep_pickup_datetime", types.StringType()) \
    .add("lpep_dropoff_datetime", types.StringType()) \
    .add("PULocationID", types.IntegerType()) \
    .add("DOLocationID", types.IntegerType()) \
    .add("passenger_count", types.DoubleType()) \
    .add("trip_distance", types.DoubleType()) \
    .add("tip_amount", types.DoubleType())
# %%
from pyspark.sql import functions as F

green_stream = green_stream \
  .select(F.from_json(F.col("value").cast('STRING'), schema).alias("data")) \
  .select("data.*")

# Row(lpep_pickup_datetime='2019-10-01 00:26:02', lpep_dropoff_datetime='2019-10-01 00:39:58', PULocationID=112, DOLocationID=196, passenger_count=1.0, trip_distance=5.88, tip_amount=0.0)
# %%
from pyspark.sql.functions import current_timestamp, window

popular_destinations = green_stream \
    .withColumn("timestamp", current_timestamp()) \
    .groupBy(
        window(F.col("timestamp"), "5 minutes"), \
        F.col("DOLocationID")) \
    .count() \
    .orderBy(F.col("count").desc())
#%%
query = popular_destinations \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()

# Most popular destination "DOLocationID" = 74
# %%
