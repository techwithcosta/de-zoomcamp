-- After ingesting CSV files into GCS bucket

-- Creating external table from CSV files (GCS)
CREATE OR REPLACE EXTERNAL TABLE `terraform-demo-412110.trips_data_all.fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://mage-zoomcamp-costatest/fhv_data_2019/fhv_tripdata_2019-*.csv']
);

-- Creating materialized table from external table (non-partitioned)
CREATE OR REPLACE TABLE trips_data_all.fhv_tripdata_non_partitioned AS
SELECT * FROM trips_data_all.fhv_tripdata;