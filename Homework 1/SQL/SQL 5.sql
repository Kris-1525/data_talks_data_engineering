SELECT
	pu."Borough" AS "PUBorough",
	pu."Zone" AS "PUZone",
	pu."service_zone" AS "PUservice_zone",
	df."Borough" AS "DOBorough",
	df."Zone" AS "DOZone",
	df."service_zone" AS "DOservice_zone",
	gt."tip_amount"
FROM
	public."green_tripdata_2019-01" as gt,
	public."taxi+_zone_lookup" as pu,
	public."taxi+_zone_lookup" as df
WHERE gt."PULocationID" = pu."LocationID"
	AND gt."DOLocationID" = df."LocationID"
	AND pu."Zone" = 'Astoria'
ORDER BY gt."tip_amount" DESC
LIMIT 100;