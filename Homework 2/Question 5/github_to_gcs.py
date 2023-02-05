import shutil
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect.filesystems import GitHub

@task
def ny_taxi_repo(github_block: GitHub):
    github_block.get_directory(
        from_path='Homework 2/Question 5/',
        local_path='./'
    )

@task
def ny_taxi_file():
    src_path = r".\Homework 2\Question 5\etl_web_to_gcs.py"
    dst_path = r".\etl_web_to_gcs.py"
    shutil.copy(src_path, dst_path)
    dir_path = r".\Homework 2"
    shutil.rmtree(dir_path, ignore_errors=False)
    
@flow
def ny_taxi_github(github_block: GitHub):
    ny_taxi_repo(github_block)
    ny_taxi_file()
    from etl_web_to_gcs import ny_taxi_etl
    ny_taxi_etl()

@flow
def ny_taxi_gh_init():
    github_block = GitHub(repository='https://github.com/Kris-1525/data_talks_data_engineering/',reference='main')
    ny_taxi_github(github_block)
    
if __name__ == '__main__':
    ny_taxi_gh_init()