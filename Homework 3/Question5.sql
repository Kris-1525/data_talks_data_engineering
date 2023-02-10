CREATE OR REPLACE TABLE
  `fair-expanse-371613.ny_taxi_fhv.ny_taxi_fhv_pc`
PARTITION BY
  DATE_TRUNC(pickup_datetime,DAY)
CLUSTER BY
  Affiliated_base_number AS (
  SELECT
    *
  FROM
    `fair-expanse-371613.ny_taxi_fhv.ny_taxi_fhv_bq`);

SELECT
  DISTINCT Affiliated_base_number
FROM
  `fair-expanse-371613.ny_taxi_fhv.ny_taxi_fhv_pc`
WHERE
  pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31';

SELECT
  DISTINCT Affiliated_base_number
FROM
  `fair-expanse-371613.ny_taxi_fhv.ny_taxi_fhv_bq`
WHERE
  pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31';