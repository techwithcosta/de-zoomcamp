#%%
import pyspark
from pyspark.sql import SparkSession, types, functions as F
print(pyspark.__version__)

import pandas as pd

#%%
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()
#%%
print(spark.version)
# 3.5.0
#%%
df_fhv_pd = pd.read_csv('data/raw/fhv_tripdata_2019-10.csv.gz', nrows=1000)

spark.createDataFrame(df_fhv_pd).schema

df_fhv_pd.head(5)
# %%

schema = types.StructType([
    types.StructField('dispatching_base_num', types.StringType(), True), 
    types.StructField('pickup_datetime', types.TimestampType(), True), 
    types.StructField('dropOff_datetime', types.TimestampType(), True), 
    types.StructField('PUlocationID', types.IntegerType(), True), 
    types.StructField('DOlocationID', types.IntegerType(), True), 
    types.StructField('SR_Flag', types.StringType(), True), 
    types.StructField('Affiliated_base_number', types.StringType(), True)
])

#%%
df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv('data/raw/fhv_tripdata_2019-10.csv.gz')

#%%
df.printSchema()
# %%
df.show()
# %%
df \
    .repartition(6) \
    .write.parquet('data/pq/fhv/2019/10', mode='overwrite')

# %%
# !ls -lh data/pq/fhv/2019/10/
# 6MB
# %%
df \
    .withColumn('pickup_date', F.to_date(df.pickup_datetime)) \
    .filter(F.col('pickup_date') == '2019-10-15') \
    .count()
# %%
df.createOrReplaceTempView("trips_data")
#%%
spark.sql("""
SELECT
    COUNT(1) AS count
FROM
    trips_data
WHERE
    DATE(pickup_datetime) == '2019-10-15'
""").show()
# How many taxi trips were there on the 15th of October?
# 62,610
# %%
df \
    .withColumn('pickup_datetime_secs', F.to_unix_timestamp(df.pickup_datetime)) \
    .withColumn('dropoff_datetime_secs', F.to_unix_timestamp(df.dropOff_datetime)) \
    .withColumn('trip_duration_hours', (F.col('dropoff_datetime_secs') - F.col('pickup_datetime_secs')) / 3600) \
    .agg({'trip_duration_hours': 'max'}) \
    .show()
# %%
spark.sql("""
SELECT
    MAX((unix_timestamp(TIMESTAMP(dropOff_datetime)) - unix_timestamp(TIMESTAMP(pickup_datetime)))) / 3600 AS max_trip_length_hours
FROM
    trips_data
""").show()
# What is the length of the longest trip in the dataset in hours?
# 631,152.5
# %%
spark.sql("""
SELECT
    pickup_datetime,
    dropOff_datetime,
    (unix_timestamp(TIMESTAMP(dropOff_datetime)) - unix_timestamp(TIMESTAMP(pickup_datetime))) / 3600 AS max_trip_length_hours
FROM
    trips_data
ORDER BY
    max_trip_length_hours DESC
LIMIT 1
""").show()
# %%
# Spark’s User Interface which shows the application's dashboard runs on which local port?
# 4040
#%%
schema = types.StructType([
    types.StructField('LocationID', types.IntegerType(), True),
    types.StructField('Borough', types.StringType(), True),
    types.StructField('Zone', types.StringType(), True),
    types.StructField('service_zone', types.StringType(), True)
])

df_zones = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv('data/raw/taxi_zone_lookup.csv')
#%%
df_zones.printSchema()
# %%
df_zones.show()
# %%
df_zones.createOrReplaceTempView("zones")
#%%
df.join(
    df_zones,
    on=[df.PUlocationID == df_zones.LocationID],
    how='left') \
    .groupBy(['PULocationID', 'Zone']) \
    .agg(F.count('*').alias('trip_count')) \
    .sort('trip_count') \
    .show(1)
# %%
spark.sql("""
SELECT
    t.PUlocationID AS pickup_location,
    zpu.Zone AS zone,
    COUNT(1) AS trip_count
FROM trips_data t
JOIN zones zpu ON t.PULocationID = zpu.LocationID
GROUP BY
    t.PUlocationID, zpu.Zone
ORDER BY trip_count
LIMIT 1
""").show()
# Jamaica Bay
# %%
