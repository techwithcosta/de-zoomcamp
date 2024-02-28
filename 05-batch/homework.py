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
!ls -lh data/pq/fhv/2019/10/
# 6MB
# %%
df \
    .withColumn('pickup_date', F.to_date(df.pickup_datetime)) \
    .withColumn('dropoff_date', F.to_date(df.dropOff_datetime)) \
    .select('pickup_date', 'dropoff_date') \
    .filter(F.col('pickup_date') == '2019-10-15') \
    .count()
# %%
df.createOrReplaceTempView("trips_data")
#%%
spark.sql("""
SELECT
    COUNT(1) as count
FROM
    trips_data
WHERE DATE(pickup_datetime) == '2019-10-15'
""").show()
# How many taxi trips were there on the 15th of October?
# 62,610
# %%
spark.sql("""
SELECT
    MAX((unix_timestamp(TIMESTAMP(dropOff_datetime)) - unix_timestamp(TIMESTAMP(pickup_datetime)))) / 3600 AS max_duration_hours
FROM trips_data
""").show()
# What is the length of the longest trip in the dataset in hours?
# 631,152.5
# %%
spark.sql("""
SELECT
    pickup_datetime,
    dropOff_datetime,
    (unix_timestamp(TIMESTAMP(dropOff_datetime)) - unix_timestamp(TIMESTAMP(pickup_datetime))) / 3600 AS max_duration_hours
FROM trips_data
ORDER BY max_duration_hours DESC
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
# %%
spark.sql("""
SELECT
    t.PUlocationID AS pickup_location,
    zpu.Zone AS zone,
    COUNT(1) AS count
FROM trips_data t
JOIN zones zpu ON t.PULocationID = zpu.LocationID
GROUP BY
    t.PUlocationID, zpu.Zone
ORDER BY count
LIMIT 1
""").show()
# Jamaica Bay
# %%
