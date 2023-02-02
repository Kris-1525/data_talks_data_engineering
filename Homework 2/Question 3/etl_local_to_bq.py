# prefect deployment build etl_local_to_bq.py:ny_taxi_etl_p --name ny_taxi_etl_d -a
# prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"
# prefect agent start -q default
# prefect deployment run ny-taxi-etl-p/ny_taxi_etl_d
from prefect_gcp import GcpCredentials
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

@task()
def ny_taxi_e1(colour: str, year: str, month: str) -> Path:
    gcs_path = f'{colour}_tripdata_{year}-{month}.parquet'
    gcs_block = GcsBucket.load('ny-taxi-gcsbuc')
    gcs_block.get_directory(from_path=gcs_path, local_path='./data')
    return Path(f'./data/{gcs_path}')

@task()
def ny_taxi_e2(gcs_path: Path) -> pd.DataFrame:
    df = pd.read_parquet(gcs_path)
    return df
    
@task()
def ny_taxi_l(df: pd.DataFrame) -> None:
    gcp_credentials_block = GcpCredentials.load("ny-taxi-gcp-cred")
    
    df.to_gbq(
        destination_table='trips_data_all_1.ny_rides_bq',
        project_id='fair-expanse-371613',
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )
    
@flow(log_prints=True)
def ny_taxi_etl_c(colour: str, year: str, month: str) -> None:
    gcs_path = ny_taxi_e1(colour, year, month)
    df = ny_taxi_e2(gcs_path)
    ny_taxi_l(df)
    
@flow(log_prints=True)
def ny_taxi_etl_p(colours:list=['yellow'], years:list=['2019'], months:list=['02','03']) -> None:
    for colour in colours:
        for year in years:
            for month in months:
                ny_taxi_etl_c(colour,year,month)
    
if __name__ == '__main__':
    ny_taxi_etl_p(colours,years,months)