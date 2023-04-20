# [START import_module]
import json
from datetime import datetime
from textwrap import dedent
import requests
# pip3 install airportsdata on terminal first
import airportsdata
import pandas as pd
import numpy as np
import os
from google.oauth2 import service_account
print(os.getcwd())

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator, BranchPythonOperator
from pathlib import Path
from google.cloud import storage
from airflow.providers.google.cloud.operators import bigquery
from airflow.contrib.operators.bigquery_get_data import BigQueryGetDataOperator
from airflow.operators.dummy import DummyOperator



# [END import_module]
default_args = {'owner': 'airflow', }

# Config Setup
project_id = 'is3107-380701'
dataset_id = 'data'
table_ids = ['flight_delay', 'weather_data']

bucket_name = 'deploy_models'
path_gcs = 'model/saved_model.pb'

# Use path of JSON Google Credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./airflow/dags/google_account_credentials.json"

# [START instantiate_dag]
with DAG(
    'is3107-GCPConnection',
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args=default_args,
    # [END default_args]
    schedule_interval="@daily",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    # helper functions
    def get_city(icao, airports):
        if icao in airports:
            return airports[icao]['city']
        else:
            return ""
        
    def round_3hr(time):
        if pd.isna(time):
            return ""
        elif int(time[11:13]) % 3 == 0:
            return time[0:13] + ":00:00"
        elif int(time[11:13]) % 3 == 1:
            return time[0:11] + str(int(time[11:13]) - 1) + ":00:00"
        elif int(time[11:13]) % 3 == 2:
            if int(time[11:13]) + 1 == 24:
                return time[0:8] + str(int(time[8:10]) + 1) + " 00:00:00"
            else:
                return time[0:11] + str(int(time[11:13]) + 1) + ":00:00"
        
    # pull api data and save into csv file
    # push csv file into GCP bucket
    # BQ to retrieve from bucket then create table (on Cloud)
    def get_delay_data(**kwargs):
        # Insert flight API key
        api_key = "Your-API-Key"
        url =f'https://airlabs.co/api/v9/delays?delay=30&type=departures&api_key={api_key}'

        ti = kwargs['ti']
        delay_cities_data = requests.get(url)
        data = delay_cities_data.json()
        
        df = pd.DataFrame(data['response'])

        Q1,Q3 = np.percentile(df['delayed'] , [25,75])
        IQR = Q3 - Q1

        df['outlier'] = np.where(df['delayed'] > 1.5 * IQR + Q3, 1, 0)

        df['dep_time_utc_3hr'] = df['dep_time_utc'].apply(lambda x: round_3hr(x))
        df['arr_time_utc_3hr'] = df['arr_time_utc'].apply(lambda x: round_3hr(x))

        airports = airportsdata.load()
        df['dep_city'] = df['dep_icao'].apply(lambda x: get_city(x, airports))
        df['arr_city'] = df['arr_icao'].apply(lambda x: get_city(x, airports))

        dep_cities = df['dep_city'].tolist()
        arr_cities = df['arr_city'].tolist()
        cities = dep_cities + arr_cities
        unique_cities = list(dict.fromkeys(cities))

        columns = df.columns.tolist()
        if 'arr_actual' not in columns:
            df['arr_actual'] = ""
            
        if 'arr_actual_ts' not in columns:
            df['arr_actual_ts'] = ""

        if 'arr_actual_utc' not in columns:
            df['arr_actual_utc'] = ""

        df.sort_index(axis = 1, inplace = True)
        ti.xcom_push('cities', unique_cities)

        # Use path of the 'prepared_data' folder
        delay_cities_path = Path("./airflow/dags/prepared_data/delay_cities.csv") 
        df.to_csv(delay_cities_path, index=False)


    def get_weather_data(**kwargs):
        ti = kwargs['ti']
        cities = ti.xcom_pull(task_ids='get_delay_data', key='cities')

        data = []
        # Insert weather api key
        api_key = 'Your-API-Key'

        for city in cities:
            api_url = f'http://api.openweathermap.org/data/2.5/forecast?q={city}&APPID={api_key}'
            response = requests.get(api_url)

            if response.status_code != 200:
                continue

            city_weather_data = response.json()['list']

            for entry in city_weather_data:
                row = {}
        
                row['city'] = city
                row['dt'] = entry['dt']
        
                main = entry['main']
                row['main_temp'] = main['temp']
                row['main_feels_like'] = main['feels_like']
                row['main_temp_min'] = main['temp_min']
                row['main_temp_max'] = main['temp_max']
                row['main_pressure'] = main['pressure']
                row['main_sea_level'] = main['sea_level']
                row['main_grnd_level'] = main['grnd_level']
                row['main_humidity'] = main['humidity']
                row['main_temp_kf'] = main['temp_kf']
        
                weather = entry['weather'][0]
                row['weather_id'] = weather['id']
                row['weather_main'] = weather['main']
                row['weather_description'] = weather['description']
                row['weather_icon'] = weather['icon']
        
                clouds = entry['clouds']
                row['clouds_all'] = clouds['all']
        
                wind = entry['wind']
                row['wind_speed'] = wind['speed']
                row['wind_deg'] = wind['deg']
                row['wind_gust'] = wind['gust']
        
                row['visibility'] = entry['visibility']
        
                row['pop'] = entry['pop']
        
                sys = entry['sys']
                row['sys_pod'] = sys['pod']
        
                row['dt_txt'] = entry['dt_txt']
        
                data.append(row)

        weather_data = pd.DataFrame(data)

        weather_data.sort_index(axis = 1, inplace = True)

        # Use path of the 'prepared_data' folder
        weather_path = Path("./airflow/dags/prepared_data/weather.csv") 
        weather_data.to_csv(weather_path, index=False)

    # upload to GCP Bucket
    def upload_blob(bucket_name, source_file_name, destination_blob_name,**kwargs):
        """Uploads a file to the bucket."""
        # The ID of your GCS bucket
        # bucket_name = "your-bucket-name"
        # The path to your file to upload
        # source_file_name = "local/path/to/file"
        # The ID of your GCS object
        # destination_blob_name = "storage-object-name"


        client_gcs = storage.Client('is3107-380701')
        bucket = client_gcs.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)


        # Optional: set a generation-match precondition to avoid potential race conditions
        # and data corruptions. The request to upload is aborted if the object's
        # generation number does not match your precondition. For a destination
        # object that does not yet exist, set the if_generation_match precondition to 0.
        # If the destination object already exists in your bucket, set instead a
        # generation-match precondition using its generation number.
        generation_match_precondition = 0

        blob.upload_from_filename(source_file_name)

        print(
            f"File {source_file_name} uploaded to {destination_blob_name}."
        )


    # [START main_flow: tasks]
    get_delay_data_task = PythonOperator(
        task_id='get_delay_data',
        python_callable=get_delay_data,
    )
    get_delay_data_task.doc_md = dedent(
        """\
    #### Get Delay Data task
    Get all the delay flights and corresponding cities from API and store as df
    """
    )
    get_weather_data_task = PythonOperator(
        task_id='get_weather_data',
        python_callable=get_weather_data,
    )
    get_weather_data_task.doc_md = dedent(
        """\
    #### Get Weather Data task
    Get all the corresponding weather of delayed flights
    """
    )

    upload_weather_to_bucket_task = PythonOperator(
        task_id='upload_weather_blob',
        python_callable=upload_blob,
        op_kwargs={"bucket_name":'gcf-sources-769425010528-us-central1', "source_file_name":'./airflow/dags/prepared_data/weather.csv', "destination_blob_name": 'weather.csv'},
    )
    upload_weather_to_bucket_task.doc_md = dedent(
        """\
    #### Get Weather Data task
    Push CSVs into bucket for bigQuery db
    """
    )

    upload_cities_to_bucket_task = PythonOperator(
        task_id='upload_cities_blob',
        python_callable=upload_blob,
        op_kwargs={"bucket_name":'gcf-sources-769425010528-us-central1', "source_file_name":'./airflow/dags/prepared_data/delay_cities.csv', "destination_blob_name": 'flight_delay.csv'},
    )
    upload_cities_to_bucket_task.doc_md = dedent(
        """\
    #### Get Weather Data task
    Push CSVs into bucket for bigQuery db
    """
    )

    create_model = bigquery.BigQueryInsertJobOperator(
        task_id='create_model',
        configuration={
            "query": {
                "query": '''
                        CREATE OR REPLACE MODEL
                        `is3107-380701.data.model`
                        OPTIONS
                        ( model_type='LOGISTIC_REG',
                        auto_class_weights=TRUE,
                        data_split_method='RANDOM',
                        data_split_eval_fraction = 0.2,
                        input_label_cols=['target'],
                        max_iterations= 30,
                        learn_rate_strategy ='constant',
                        learn_rate =0.2,
                        num_trials=5,
                        HPARAM_TUNING_ALGORITHM = 'RANDOM_SEARCH',
                        L2_REG = hparam_range(0, 10),
                        L1_reg=hparam_range(0, 10)) AS



                        SELECT 
                        IFNULL(delay.airline_iata, 'null') as airline_iata,
                        IFNULL(delay.dep_iata, 'null') as dep_iata,
                        IFNULL(delay.arr_iata, 'null') as arr_iata,
                        IFNULL(delay.duration, 0) as duration,
                        IFNULL(EXTRACT(HOUR FROM delay.dep_time_utc) * 60 +  EXTRACT(MINUTE FROM delay.dep_time_utc), 0) as dep_time_float,
                        IFNULL(dep.weather_description, 'null') as dep_desc,
                        IFNULL(arr.weather_description, 'null') as arr_desc,
                        case
                          when delay.delayed > 45 THEN 1
                          ELSE 0
                        END as target
                        FROM `is3107-380701.data.flight_delay` as delay
                        LEFT JOIN `is3107-380701.data.weather_data` as dep
                        ON delay.dep_city = dep.city and delay.dep_time_utc_3hr = dep.dt_txt
                        LEFT JOIN `is3107-380701.data.weather_data` as arr
                        ON delay.dep_city = arr.city and delay.dep_time_utc_3hr = arr.dt_txt
                        where delay.outlier = 0
                        ''',
                "useLegacySql": False,
            }
        },
        gcp_conn_id = 'is3107_connection',
        dag = dag

    )

    evaluate_model = bigquery.BigQueryInsertJobOperator(
        task_id='evaluate_model',
        configuration={
            "query": {
                "query": '''
                        CREATE OR REPLACE TABLE
                        `is3107-380701.data.evaluation` AS
                        SELECT * FROM
                        ML.EVALUATE(MODEL `is3107-380701.data.model`)
                        ''',
                "useLegacySql": False,
            }
        },
        gcp_conn_id = 'is3107_connection',
        dag = dag
    )

    get_metrics = BigQueryGetDataOperator(
        task_id = 'get_metrics',
        gcp_conn_id = 'is3107_connection',
        dataset_id = 'data',
        table_id = 'evaluation'
    )

    export_model = bigquery.BigQueryInsertJobOperator(
        task_id = 'export_model',
        configuration={
            "query": {
                "query": '''
                         EXPORT MODEL `is3107-380701.data.model`
                         OPTIONS(URI = 'gs://deploy_models/model')
                         ''',
                "useLegacySql": False,
            }
        },
        gcp_conn_id = 'is3107_connection',
        dag = dag
    )
                

    do_nothing = DummyOperator(task_id = 'do_nothing')

    def validate_model(**kwargs):
        ti = kwargs['ti']
        table = ti.xcom_pull(task_ids='get_metrics')
        df = pd.DataFrame(table, columns = ['trial_id','precision','recall','accuracy','f1_score','log_loss','roc_auc'])
        df = df.apply(pd.to_numeric)
        metrics = df.loc[df['roc_auc'].idxmax()]
        if metrics['precision'] > 0.65 and metrics['recall'] > 0.65 and metrics['f1_score'] > 0.65:
            return 'export_model'

        return 'do_nothing'

    validate_model = BranchPythonOperator(
    task_id='validate_model',
    python_callable=validate_model,)


    def fetch_prediction_model():
        
        client_gcs = storage.Client()
        bucket_name = 'deploy_models'
        model_path_gcs = 'model/saved_model.pb'
        
        bucket = client_gcs.bucket(bucket_name)
        blob = bucket.blob(model_path_gcs)
        model_as_bytes = blob.download_as_bytes()

        # Use path of the 'models' folder
        with open('./airflow/dags/models/saved_model.pb', 'wb') as f:
            f.write(model_as_bytes)

        model_path_gcs = 'model/fingerprint.pb'
        blob = bucket.blob(model_path_gcs)
        model_as_bytes = blob.download_as_bytes()

        # Use path of the 'models' folder
        with open('./airflow/dags/models/fingerprint.pb', 'wb') as f:
            f.write(model_as_bytes)

        model_path_gcs = 'model/explanation_metadata.json'
        blob = bucket.blob(model_path_gcs)
        model_as_bytes = blob.download_as_bytes()

        # Use path of the 'models' folder
        with open('./airflow/dags/models/explanation_metadata.json', 'wb') as f:
            f.write(model_as_bytes)

        model_path_gcs = 'model/variables/variables.data-00000-of-00001'
        blob = bucket.blob(model_path_gcs)
        model_as_bytes = blob.download_as_bytes()

        # Use path of the 'models/variables' folder
        with open('./airflow/dags/models/variables/variables.data-00000-of-00001', 'wb') as f:
            f.write(model_as_bytes)

        model_path_gcs = 'model/variables/variables.index'
        blob = bucket.blob(model_path_gcs)
        model_as_bytes = blob.download_as_bytes()

        # Use path of the 'models/assets' folder
        with open('./airflow/dags/models/variables/variables.index', 'wb') as f:
            f.write(model_as_bytes)

        model_path_gcs = 'model/assets/airline_iata.txt'
        blob = bucket.blob(model_path_gcs)
        model_as_bytes = blob.download_as_bytes()
        
        # Use path of the 'models/assets' folder
        with open('./airflow/dags/models/assets/airline_iata.txt', 'wb') as f:
            f.write(model_as_bytes)

        model_path_gcs = 'model/assets/arr_iata.txt'
        blob = bucket.blob(model_path_gcs)
        model_as_bytes = blob.download_as_bytes()
        
        # Use path of the 'models/assets' folder
        with open('./airflow/dags/models/assets/arr_iata.txt', 'wb') as f:
            f.write(model_as_bytes)

        model_path_gcs = 'model/assets/arr_desc.txt'
        blob = bucket.blob(model_path_gcs)
        model_as_bytes = blob.download_as_bytes()
        
        # Use path of the 'models/assets' folder
        with open('./airflow/dags/models/assets/arr_desc.txt', 'wb') as f:
            f.write(model_as_bytes)

        model_path_gcs = 'model/assets/dep_iata.txt'
        blob = bucket.blob(model_path_gcs)
        model_as_bytes = blob.download_as_bytes()

        # Use path of the 'models/assets' folder
        with open('./airflow/dags/models/assets/dep_iata.txt', 'wb') as f:
            f.write(model_as_bytes)

        model_path_gcs = 'model/assets/dep_desc.txt'
        blob = bucket.blob(model_path_gcs)
        model_as_bytes = blob.download_as_bytes()
        
        # Use path of the 'models/assets' folder
        with open('./airflow/dags/models/assets/dep_desc.txt', 'wb') as f:
            f.write(model_as_bytes)



    fetch_prediction_model = PythonOperator(
        task_id='fetch_prediction_model',
        python_callable=fetch_prediction_model,
    )

    get_delay_data_task >> get_weather_data_task >> [upload_cities_to_bucket_task, upload_weather_to_bucket_task] >> create_model >> evaluate_model >> get_metrics >> validate_model >> [export_model,do_nothing]

    export_model >> fetch_prediction_model

