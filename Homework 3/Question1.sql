CREATE EXTERNAL TABLE `fair-expanse-371613.ny_taxi_fhv.ny_taxi_fhv_table`
  OPTIONS (
    format ="CSV",
    uris = ["gs://dtc_data_lake_fair-expanse-371613/fhv/fhv_tripdata_2019-01.csv", "gs://dtc_data_lake_fair-expanse-371613/fhv/fhv_tripdata_2019-02.csv", "gs://dtc_data_lake_fair-expanse-371613/fhv/fhv_tripdata_2019-03.csv", "gs://dtc_data_lake_fair-expanse-371613/fhv/fhv_tripdata_2019-04.csv", "gs://dtc_data_lake_fair-expanse-371613/fhv/fhv_tripdata_2019-05.csv", "gs://dtc_data_lake_fair-expanse-371613/fhv/fhv_tripdata_2019-06.csv", "gs://dtc_data_lake_fair-expanse-371613/fhv/fhv_tripdata_2019-07.csv", "gs://dtc_data_lake_fair-expanse-371613/fhv/fhv_tripdata_2019-08.csv", "gs://dtc_data_lake_fair-expanse-371613/fhv/fhv_tripdata_2019-09.csv", "gs://dtc_data_lake_fair-expanse-371613/fhv/fhv_tripdata_2019-10.csv", "gs://dtc_data_lake_fair-expanse-371613/fhv/fhv_tripdata_2019-11.csv" , "gs://dtc_data_lake_fair-expanse-371613/fhv/fhv_tripdata_2019-12.csv"]
    );

CREATE TABLE ny_taxi_fhv.ny_taxi_fhv_bq AS (
  SELECT
    *
  FROM
    fair-expanse-371613.ny_taxi_fhv.ny_taxi_fhv_table
);