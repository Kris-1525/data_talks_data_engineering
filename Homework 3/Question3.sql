SELECT
  COUNT(1)
FROM
  fair-expanse-371613.ny_taxi_fhv.ny_taxi_fhv_bq
WHERE
  ny_taxi_fhv_bq.PUlocationID IS NULL
  AND ny_taxi_fhv_bq.DOlocationID IS NULL;