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
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    return df

@task()
def ny_taxi_t2(colour: str, datafile: str, df: pd.DataFrame) -> Path:
    path = Path(f'{datafile}.parquet')
    df.to_parquet(path, compression='gzip')
    return path
    
@flow()
def ny_taxi_etl() -> None:
    colour = 'yellow'
    year = '2019'
    months = ['02','03']
    for month in months:
        datafile = f'{colour}_tripdata_{year}-{month}'
        dataseturl = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{colour}/{datafile}.csv.gz'

        df = ny_taxi_e(dataseturl)
        df_clean = ny_taxi_t1(df)
        path = ny_taxi_t2(colour, datafile, df_clean)
    
if __name__ == '__main__':
    ny_taxi_etl()