
SELECT
	DATE("lpep_pickup_datetime"),
	"trip_distance"
FROM
	public."green_tripdata_2019-01"
ORDER BY "trip_distance" DESC
LIMIT 100;