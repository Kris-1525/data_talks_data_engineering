import pandas as pd
import argparse
import os
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    dbname = params.dbname
    tbname = params.tbname
    
    ny_taxi_engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')
    ny_taxi_engine.connect()
    
    # print(pd.io.sql.get_schema(ny_taxi_pd,name='yellow_ny_taxi_jan',con=ny_taxi_engine))
    
    ny_taxi_pd_iter = pd.read_csv(filepath_or_buffer='taxi+_zone_lookup.csv',iterator=True,chunksize=10000)
    
    ny_taxi_pd = next(ny_taxi_pd_iter)

    ny_taxi_pd.to_sql(name=f'{tbname}',con=ny_taxi_engine,if_exists='replace')
    
    while True:
        ny_taxi_pd = next(ny_taxi_pd_iter,"None")
    
        if isinstance(ny_taxi_pd,str):
            break
        
        ny_taxi_pd.to_sql(name=f'{tbname}',con=ny_taxi_engine,if_exists='append')
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = 'Ingesting CSV data into SQL database')
    parser.add_argument('--user', help='user for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--dbname', help='database name for postgres')
    parser.add_argument('--tbname', help='table name for postgres')
    args = parser.parse_args()
    main(args)