from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import pickle

sys.path.insert(0, '/mnt/c/code/Projekt Data Scraping/src/Python code/etl')

from extract_olx import OlxScraper
from extract_otodom import OtodomScraper
from transform import OlxTransform, OtoDomTransform, JoinEstateData
from load import FactLoad

olx_pickle_path = '/mnt/c/code/Projekt Data Scraping/data/olx_urls.pkl'
otodom_pickle_path = '/mnt/c/code/Projekt Data Scraping/data/otodom_urls.pkl'
olx_data_path = '/mnt/c/code/Projekt Data Scraping/data/OLX/OLX_Data.csv'
otodom_data_path ='/mnt/c/code/Projekt Data Scraping/data/OLX/OtoDom_Data.csv'

OlxExtractionObject = OlxScraper(output_path=olx_data_path)
OtoDomExtractionObject = OtodomScraper(key='4JKqPCoRE7cVNqIQeP-Pf', output_path=otodom_data_path)



default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 14),
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='etl_dag',
    default_args=default_args,
    schedule_interval='0 19 * * *',
    catchup=False,
    max_active_tasks=4
) as dag:

    #Extract OLX
    def get_all_olx_urls(**kwargs):
        OlxExtractionObject.get_all_urls(print_page_numbers=True)
        with open(olx_pickle_path, 'wb') as f:
            pickle.dump(OlxExtractionObject.cities_individual_urls, f)
    
    OLX_get_urls_task = PythonOperator(
        task_id='OLX_get_urls_task',
        python_callable=get_all_olx_urls,
    )

    def scrap_olx_data(**kwargs):
        with open(olx_pickle_path, 'rb') as f:
            OlxExtractionObject.cities_individual_urls = pickle.load(f)

        if os.path.exists(olx_pickle_path):
            os.remove(olx_pickle_path)
            print(f"{olx_pickle_path} has been deleted.")
        else:
            print(f"{olx_pickle_path} does not exist.")

        OlxExtractionObject.scrap_data(print_page_numbers=True)
    
    OLX_scrap_data_task = PythonOperator(
        task_id='OLX_scrap_data_task',
        python_callable=scrap_olx_data,
    )

    #Extract OtoDom
    def get_all_otodom_urls(**kwargs):
        OtoDomExtractionObject.get_all_urls(print_page_numbers=True)
        with open(otodom_pickle_path, 'wb') as f:
            pickle.dump(OtoDomExtractionObject.cities_individual_urls, f)
    
    OtoDom_get_urls_task = PythonOperator(
        task_id='OtoDom_get_urls_task',
        python_callable=get_all_otodom_urls,
    )

    def scrap_otodom_data(**kwargs):
        with open(otodom_pickle_path, 'rb') as f:
            OtoDomExtractionObject.cities_individual_urls = pickle.load(f)

        if os.path.exists(otodom_pickle_path):
            os.remove(otodom_pickle_path)
            print(f"{otodom_pickle_path} has been deleted.")
        else:
            print(f"{otodom_pickle_path} does not exist.")

        OtoDomExtractionObject.scrap_data(print_page_numbers=True)
    
    OtoDom_scrap_data_task = PythonOperator(
        task_id='OtoDom_scrap_data_task',
        python_callable=scrap_otodom_data,
    )

    #Transform
    OLX_transform_task = PythonOperator(
        task_id='OLX_transform_task',
        python_callable=OlxTransform,
    )

    OtoDom_transform_task = PythonOperator(
        task_id='OtoDom_transform_task',
        python_callable=OtoDomTransform,
    )

    MergeData_task = PythonOperator(
        task_id='MergeData_task',
        python_callable=JoinEstateData
    )

    #Load
    LoadData_task = PythonOperator(
        task_id="LoadData_task",
        python_callable=FactLoad

    )
    
    OtoDom_get_urls_task >> OtoDom_scrap_data_task >> OtoDom_transform_task
    OLX_get_urls_task >> OLX_scrap_data_task >> OLX_transform_task
    OLX_transform_task >> MergeData_task << OtoDom_transform_task
    MergeData_task >> LoadData_task