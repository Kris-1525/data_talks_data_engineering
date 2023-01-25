# docker run -d --name pythonsql --network=pgnet kristmateaus/pythonsql:1
FROM python:3.9.16
RUN pip install pandas
RUN pip install sqlalchemy
RUN pip install psycopg2
RUN mkdir 'Taxi Data Load'
WORKDIR '/Taxi Data Load'
COPY Taxi_Data_Load_1.py .
COPY Taxi_Data_Load_2.py .
# COPY yellow_tripdata_2022-01.csv .
RUN curl -sSL https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz > green_tripdata_2019-01.csv.gz
RUN curl -sSL https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv > taxi+_zone_lookup.csv
CMD sleep 1m && python Taxi_Data_Load_1.py --user=root --password=root --host=postgresql --port=5432 --dbname=ny_taxi --tbname=green_tripdata_2019-01 && python Taxi_Data_Load_2.py --user=root --password=root --host=postgresql --port=5432 --dbname=ny_taxi --tbname=taxi+_zone_lookup