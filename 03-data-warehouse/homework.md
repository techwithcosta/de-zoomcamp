## Week 3 Homework
ATTENTION: At the end of the submission form, you will be required to include a link to your GitHub repository or other public code-hosting site. This repository should contain your code for solving the homework. If your solution includes code that is not in file format (such as SQL queries or shell commands), please include these directly in the README file of your repository.

<b><u>Important Note:</b></u> <p> For this homework we will be using the 2022 Green Taxi Trip Record Parquet Files from the New York
City Taxi Data found here: </br> https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page </br>
If you are using orchestration such as Mage, Airflow or Prefect do not load the data into Big Query using the orchestrator.</br> 
Stop with loading the files into a bucket. </br></br>
<u>NOTE:</u> You will need to use the PARQUET option files when creating an External Table</br>

<b>SETUP:</b></br>
Create an external table using the Green Taxi Trip Records Data for 2022. </br>
Create a table in BQ using the Green Taxi Trip Records for 2022 (do not partition or cluster this table). </br>
</p>

>SOLUTION
```
PYTHON CODE

# download parquet files automatically
import os

year = '2022'
months = [str(month).zfill(2) for month in range(1, 13)]

for month in months:
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month}.parquet'
    os.system(f'wget {url}')

SQL CODE

-- Creating external table from parquet files (GCS)
CREATE OR REPLACE EXTERNAL TABLE `terraform-demo-412110.ny_taxi.external_green_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://mage-zoomcamp-costatest/nyc-tl-data/trip data/green_tripdata_2022-*.parquet']
);

-- Creating materialized table from external table (non-partitioned)
CREATE OR REPLACE TABLE ny_taxi.green_tripdata_non_partitioned AS
SELECT * FROM ny_taxi.external_green_tripdata LIMIT 5;

-- Create partitioned table from external table
CREATE OR REPLACE TABLE ny_taxi.green_tripdata_partitioned
PARTITION BY
  DATE(lpep_pickup_datetime) AS
SELECT * FROM ny_taxi.external_green_tripdata;

-- Checking partitions
SELECT table_name, partition_id, total_rows
FROM `ny_taxi.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'green_tripdata_partitioned'
ORDER BY total_rows DESC;

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE ny_taxi.green_tripdata_partitioned_clustered
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PULocationID AS
SELECT * FROM ny_taxi.external_green_tripdata;
```

## Question 1:
Question 1: What is count of records for the 2022 Green Taxi Data??
- 65,623,481
- 840,402
- 1,936,423
- 253,647

>SOLUTION
```
SELECT COUNT(1) AS total_trips FROM `ny_taxi.external_green_tripdata`
```
>ANSWER
```
840,402
```

## Question 2:
Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.</br> 
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

- 0 MB for the External Table and 6.41MB for the Materialized Table
- 18.82 MB for the External Table and 47.60 MB for the Materialized Table
- 0 MB for the External Table and 0MB for the Materialized Table
- 2.14 MB for the External Table and 0MB for the Materialized Table

>SOLUTION
```
SELECT COUNT(DISTINCT PULocationID) AS unique_pulocationid FROM ny_taxi.external_green_tripdata;
-- 0 B

SELECT COUNT(DISTINCT PULocationID) AS unique_pulocationid FROM ny_taxi.green_tripdata_non_partitioned;
-- 6.41 MB
```
>ANSWER
```
0 MB for the External Table and 6.41MB for the Materialized Table
```

## Question 3:
How many records have a fare_amount of 0?
- 12,488
- 128,219
- 112
- 1,622

>SOLUTION
```
SELECT COUNT(1) AS total_trips FROM ny_taxi.external_green_tripdata
WHERE fare_amount = 0
```
>ANSWER
```
1,622
```

## Question 4:
What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)
- Cluster on lpep_pickup_datetime Partition by PUlocationID
- Partition by lpep_pickup_datetime  Cluster on PUlocationID
- Partition by lpep_pickup_datetime and Partition by PUlocationID
- Cluster on by lpep_pickup_datetime and Cluster on PUlocationID

>ANSWER
```
Partition by lpep_pickup_datetime Cluster on PUlocationID
```

## Question 5:
Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime
06/01/2022 and 06/30/2022 (inclusive)</br>

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? </br>

Choose the answer which most closely matches.</br> 

- 22.82 MB for non-partitioned table and 647.87 MB for the partitioned table
- 12.82 MB for non-partitioned table and 1.12 MB for the partitioned table
- 5.63 MB for non-partitioned table and 0 MB for the partitioned table
- 10.31 MB for non-partitioned table and 10.31 MB for the partitioned table

>SOLUTION
```
SELECT DISTINCT PULocationID FROM ny_taxi.green_tripdata_non_partitioned
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';
-- 12.82 MB

SELECT DISTINCT PULocationID FROM ny_taxi.green_tripdata_partitioned
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';
-- 1.12 MB
```
>ANSWER
```
12.82 MB for non-partitioned table and 1.12 MB for the partitioned table
```

## Question 6: 
Where is the data stored in the External Table you created?

- Big Query
- GCP Bucket
- Big Table
- Container Registry

>ANSWER
```
GCP Bucket
```

## Question 7:
It is best practice in Big Query to always cluster your data:
- True
- False

>ANSWER
```
False
```

## (Bonus: Not worth points) Question 8:
No Points: Write a `SELECT count(*)` query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

>SOLUTION
```
SELECT count(*) FROM `ny_taxi.green_tripdata_non_partitioned`
-- 0 B
```
>ANSWER
```
0 B, because the table is cached
```

## Submitting the solutions

* Form for submitting: https://courses.datatalks.club/de-zoomcamp-2024/homework/hw3


