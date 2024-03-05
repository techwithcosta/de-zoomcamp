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

# %%
df_green.createOrReplaceTempView("green")
# %%
df_green_revenue = spark.sql("""
SELECT
    -- Revenue grouping
    date_trunc('hour', lpep_pickup_datetime) AS hour,
    PULocationID AS zone,

    -- Revenue calculation
    SUM(total_amount) AS amount,
    COUNT(1) AS number_records
FROM
    green
WHERE
    lpep_pickup_datetime >= '2020-01-01 00:00:00'
GROUP BY
    1, 2
""")
# %%
df_green_revenue.write.parquet('data/report/revenue/green', mode='overwrite')
# %%
df_yellow = spark.read.parquet('data/pq/yellow/*/*')

# %%
df_yellow.createOrReplaceTempView("yellow")
# %%
df_yellow_revenue = spark.sql("""
SELECT
    -- Revenue grouping
    date_trunc('hour', tpep_pickup_datetime) AS hour,
    PULocationID AS zone,

    -- Revenue calculation
    SUM(total_amount) AS amount,
    COUNT(1) AS number_records
FROM
    yellow
WHERE
    tpep_pickup_datetime >= '2020-01-01 00:00:00'
GROUP BY
    1, 2
""")
# %%
df_yellow_revenue.write.parquet('data/report/revenue/yellow', mode='overwrite')
#%%
df_green_revenue_tmp = df_green_revenue \
    .withColumnRenamed('amount', 'green_amount') \
    .withColumnRenamed('number_records', 'green_number_records')

df_yellow_revenue_tmp = df_yellow_revenue \
    .withColumnRenamed('amount', 'yellow_amount') \
    .withColumnRenamed('number_records', 'yellow_number_records')
# %%
df_green_yellow_revenue = df_green_revenue_tmp.join(df_yellow_revenue_tmp,
                                                on=['hour','zone'],
                                                how='outer')
#%%
df_green_yellow_revenue.write.parquet('data/report/revenue/total')
#%%
df_green_yellow_revenue.show()
# %%
# merge small table into big table
df_zones = spark.read.parquet('data/pq/zones/')
df_result = df_green_yellow_revenue.join(df_zones,
                             df_green_yellow_revenue.zone == df_zones.LocationID) \
                             .drop('LocationID', 'zone')
df_result.write.parquet('data/tmp/revenue-zones', mode='overwrite')

# %%
