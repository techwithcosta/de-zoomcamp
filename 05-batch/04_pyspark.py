#%%

# add spark to sys.path
# instead of creating env vars on .bashrc with export

# export PYTHONPATH="/home/costa/spark/spark-3.5.0-bin-hadoop3/python/:$PYTHONPATH"
# export PYTHONPATH="/home/costa/spark/spark-3.5.0-bin-hadoop3/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH"

# export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
# export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH"
#%%
# import sys
# import os

# # could also use findspark package
# # this approach points to the right location
# SPARK_HOME = os.environ.get('SPARK_HOME')
# sys.path.append(f'{SPARK_HOME}/python/')
# sys.path.append(f'{SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip')

#%%

import pyspark
from pyspark.sql import SparkSession, types

print(pyspark.__version__)

#%%

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

#%%

df = spark.read \
    .option("header", "true") \
    .csv('data/raw/taxi_zone_lookup.csv')

df.show()

#%%

df.write.parquet('data/zones', mode='overwrite')

#%%

df = spark.read \
    .option("header", "true") \
    .csv('data/raw/fhvhv_tripdata_2021-01.csv')

print(df.schema)
df.head(10)

#%%

schema = types.StructType([
    types.StructField('hvfhs_license_num', types.StringType(), True),
    types.StructField('dispatching_base_num', types.StringType(), True),
    types.StructField('pickup_datetime', types.TimestampType(), True),
    types.StructField('dropoff_datetime', types.TimestampType(), True),
    types.StructField('PULocationID', types.IntegerType(), True),
    types.StructField('DOLocationID', types.IntegerType(), True),
    types.StructField('SR_Flag', types.StringType(), True)
])

df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv('data/raw/fhvhv_tripdata_2021-01.csv')

df.head(10)

# %%
# repartition is a lazy command
df = df.repartition(24)
#%%
df.write.parquet('data/fhvhv/2021/01/', mode='overwrite')
# %%
df = spark.read.parquet('data/fhvhv/2021/01')
#%%
df.printSchema()

# %%
df \
    .select('pickup_datetime', 'dropoff_datetime', 'PULocationID', 'DOLocationID') \
    .filter(df.hvfhs_license_num == 'HV0003') \
    .show()
#%%
df.show()
#%%
def crazy_stuff(base_num):
    num = int(base_num[1:])
    if num % 7 == 0:
        return f's/{num:03x}'
    elif num % 3 == 0:
        return f'a/{num:03x}'
    else:
        return f'e/{num:03x}'
#%%
crazy_stuff('B02884')
#%%
from pyspark.sql import functions as F
crazy_stuff_udf = F.udf(crazy_stuff, returnType=types.StringType())
#%%
# had to use spark 3.5.0 with python 3.12
df \
    .withColumn('pickup_date', F.to_date(df.pickup_datetime)) \
    .withColumn('dropoff_date', F.to_date(df.dropoff_datetime)) \
    .withColumn('base_id', crazy_stuff_udf(df.dispatching_base_num)) \
    .select('base_id', 'pickup_date', 'dropoff_date', 'PULocationID', 'DOLocationID') \
    .show()
# %%
