SELECT
	COUNT(1)
FROM
	public."green_tripdata_2019-01" as gt1
WHERE
	DATE(gt1."lpep_pickup_datetime") = '2019-01-01'
	AND gt1."passenger_count" = 2
LIMIT 100;