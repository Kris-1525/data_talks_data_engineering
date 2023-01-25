SELECT
	COUNT(1)
FROM
	public."green_tripdata_2019-01"
WHERE
	DATE("lpep_pickup_datetime") = '2019-01-15'
	AND DATE("lpep_dropoff_datetime") = '2019-01-15'
LIMIT 100;