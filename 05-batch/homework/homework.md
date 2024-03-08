## Week 5 Homework 

In this homework we'll put what we learned about Spark in practice.

For this homework we will be using the FHV 2019-10 data found here. [FHV Data](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-10.csv.gz)

### Question 1: 

**Install Spark and PySpark** 

- Install Spark
- Run PySpark
- Create a local spark session
- Execute spark.version.

What's the output?

> [!NOTE]
> To install PySpark follow this [guide](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/05-batch/setup/pyspark.md)

>SOLUTION
```
print(spark.version)
```
>ANSWER ✅
```
3.5.0
```

### Question 2: 

**FHV October 2019**

Read the October 2019 FHV into a Spark Dataframe with a schema as we did in the lessons.

Repartition the Dataframe to 6 partitions and save it to parquet.

What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

- 1MB
- 6MB
- 25MB
- 87MB

>SOLUTION
```
df \
    .repartition(6) \
    .write.parquet('data/pq/fhv/2019/10', mode='overwrite')

!ls -lh data/pq/fhv/2019/10/
```
>ANSWER ✅
```
6MB (6,4M)
```

### Question 3: 

**Count records** 

How many taxi trips were there on the 15th of October?

Consider only trips that started on the 15th of October.

- 108,164
- 12,856
- 452,470
- 62,610

> [!IMPORTANT]
> Be aware of columns order when defining schema

>SOLUTION
```
PYSPARK

df \
    .withColumn('pickup_date', F.to_date(df.pickup_datetime)) \
    .filter(F.col('pickup_date') == '2019-10-15') \
    .count()

PYSPARK.SQL

spark.sql("""
SELECT
    COUNT(1) AS count
FROM
    trips_data
WHERE
    DATE(pickup_datetime) == '2019-10-15'
""").show()
```
>ANSWER ✅
```
62,610
```

### Question 4: 

**Longest trip for each day** 

What is the length of the longest trip in the dataset in hours?

- 631,152.50 Hours
- 243.44 Hours
- 7.68 Hours
- 3.32 Hours

>SOLUTION
```
PYSPARK

df \
    .withColumn('pickup_datetime_secs', F.to_unix_timestamp(df.pickup_datetime)) \
    .withColumn('dropoff_datetime_secs', F.to_unix_timestamp(df.dropOff_datetime)) \
    .withColumn('trip_duration_hours', (F.col('dropoff_datetime_secs') - F.col('pickup_datetime_secs')) / 3600) \
    .agg({'trip_duration_hours': 'max'}) \
    .show()

PYSPARK.SQL

spark.sql("""
SELECT
    MAX((unix_timestamp(TIMESTAMP(dropOff_datetime)) - unix_timestamp(TIMESTAMP(pickup_datetime)))) / 3600 AS max_trip_length_hours
FROM
    trips_data
""").show()
```
>ANSWER ✅
```
631,152.50 Hours
```

### Question 5: 

**User Interface**

Spark’s User Interface which shows the application's dashboard runs on which local port?

- 80
- 443
- 4040
- 8080

>SOLUTION
```
localhost:4040
```
>ANSWER ✅
```
4040
```

### Question 6: 

**Least frequent pickup location zone**

Load the zone lookup data into a temp view in Spark</br>
[Zone Data](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv)

Using the zone lookup data and the FHV October 2019 data, what is the name of the LEAST frequent pickup location Zone?</br>

- East Chelsea
- Jamaica Bay
- Union Sq
- Crown Heights North

>SOLUTION
```
PYSPARK

df.join(
    df_zones,
    on=[df.PUlocationID == df_zones.LocationID],
    how='left') \
    .groupBy(['PULocationID', 'Zone']) \
    .agg(F.count('*').alias('trip_count')) \
    .sort('trip_count') \
    .show(1)

PYSPARK.SQL

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
```
>ANSWER ✅
```
Jamaica Bay
```

## Submitting the solutions

- Form for submitting: https://courses.datatalks.club/de-zoomcamp-2024/homework/hw5
- Deadline: See the website
