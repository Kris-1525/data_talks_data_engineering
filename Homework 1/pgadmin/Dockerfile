# docker build -t kristmateaus/pgadmin:1 .
# docker run -d -p 8082:80 --name pgadmin --network=pgnet kristmateaus/pgadmin:1
FROM dpage/pgadmin4:2022-12-14-1
ENV PGADMIN_DEFAULT_EMAIL="admin@admin.com"
ENV PGADMIN_DEFAULT_PASSWORD="root"
EXPOSE 80