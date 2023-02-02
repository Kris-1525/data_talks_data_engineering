# prefect deployment build etl_web_to_gcs.py:ny_taxi_etl --name ny_taxi_etl_d --cron '0 5 1 * *' -a
# prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"
# prefect agent start -q default
# prefect deployment run ny-taxi-etl/ny_taxi_etl_d
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

@task()
def ny_taxi_e(dataset_url: str) -> pd.DataFrame:
    df = pd.read_csv(filepath_or_buffer=dataset_url, compression='gzip')
    print('\n\n')
    print("The number of rows to be uploaded to GCS "+f"{df.shape[0]}")
    print('\n\n')
    return df

@task()
def ny_taxi_t1(df: pd.DataFrame) -> pd.DataFrame:
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    return df

@task()
def ny_taxi_t2(colour: str, datafile: str, df: pd.DataFrame) -> Path:
    path = Path(f'{datafile}.parquet')
    df.to_parquet(path, compression='gzip')
    return path

@task()
def ny_taxi_l(path: str, datafile: str) -> None:
    gcp_block = GcsBucket.load("ny-taxi-gcsbuc")
    gcp_block.upload_from_path(from_path=path, to_path=Path(f'{datafile}.parquet'))
    
@flow()
def ny_taxi_etl() -> None:
    colour = 'green'
    year = '2020'
    month = '01'
    datafile = f'{colour}_tripdata_{year}-{month}'
    dataseturl = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{colour}/{datafile}.csv.gz'
    
    df = ny_taxi_e(dataseturl)
    df_clean = ny_taxi_t1(df)
    path = ny_taxi_t2(colour, datafile, df_clean)
    ny_taxi_l(path, datafile)
    
if __name__ == '__main__':
    ny_taxi_etl()