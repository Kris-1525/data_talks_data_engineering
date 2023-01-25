# docker build -t kristmateaus/prostgresql:1 .
# docker network create pgnet
# docker run -d -p 5434:5432 -v 'd:/Data Talks/Data Engineering/Week 1/Part 1/data':/var/lib/postgresql/data --name postgresql --network=pgnet kristmateaus/prostgresql:1
FROM postgres:13.9-alpine3.17
ENV POSTGRES_USER="root"
ENV POSTGRES_PASSWORD="root"
ENV POSTGRES_DB="ny_taxi"
EXPOSE 5432