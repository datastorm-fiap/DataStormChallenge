from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.utils.dates import days_ago
import pandas as pd
import requests
import time
import json
from datetime import datetime

# Função para coletar dados das subprefeituras e salvar em formato adequado para o BigQuery
def collect_weather_data(**kwargs):
    # Baixar o CSV das subprefeituras diretamente do Google Cloud Storage
    subprefeituras = pd.read_csv('/tmp/subprefeituras-sp.csv', sep=';', header=0)
    subprefeituras.columns = subprefeituras.columns.str.strip().str.lower()

    API_KEY = '0f840aa5645d3e1299050cab0d8c1e6e'  # Substitua pela sua chave da API
    BASE_URL = 'https://history.openweathermap.org/data/2.5/history/city'
    
    results = []
    for index, row in subprefeituras.iterrows():
        lat = round(row['latitude'], 2)
        lon = round(row['longitude'], 2)
        subprefeitura_nome = row['subprefeitura']
        url = f"{BASE_URL}?lat={lat:.2f}&lon={lon:.2f}&appid={API_KEY}"
        
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            if 'list' in data:
                for entry in data['list']:
                    result = {
                        'subprefeitura': subprefeitura_nome,
                        'latitude': lat,
                        'longitude': lon,
                        'timestamp': datetime.utcfromtimestamp(entry['dt']).isoformat(),  # Converter para timestamp ISO
                        'temperature': entry['main'].get('temp'),
                        'feels_like': entry['main'].get('feels_like'),
                        'pressure': entry['main'].get('pressure'),
                        'humidity': entry['main'].get('humidity'),
                        'temp_min': entry['main'].get('temp_min'),
                        'temp_max': entry['main'].get('temp_max'),
                        'wind_speed': entry['wind'].get('speed'),
                        'wind_deg': entry['wind'].get('deg'),
                        'wind_gust': entry['wind'].get('gust'),
                        'clouds_all': entry['clouds'].get('all'),
                        'weather_id': entry['weather'][0].get('id') if entry['weather'] else None,
                        'weather_main': entry['weather'][0].get('main') if entry['weather'] else None,
                        'weather_description': entry['weather'][0].get('description') if entry['weather'] else None,
                        'weather_icon': entry['weather'][0].get('icon') if entry['weather'] else None,
                    }
                    print(f"Resultado formatado: {result}")  # Print para verificar o formato
                    results.append(result)
        else:
            print(f"Erro: {response.status_code} para lat={lat}, lon={lon}")

        time.sleep(1)  # Limite de 60 chamadas por minuto

    # Salvar o JSON em formato NEWLINE_DELIMITED_JSON adequado para o BigQuery
    file_name = f'/tmp/historical_weather_data.json'
    with open(file_name, 'w') as f:
        for entry in results:
            f.write(json.dumps(entry) + '\n')  # Escreve cada objeto JSON em uma nova linha

    print(f"Arquivo JSON salvo em: {file_name}")
    return file_name

# Definir os argumentos padrão da DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# Definir a DAG
with DAG(
    dag_id='weather_data_pipeline',
    default_args=default_args,
    description='DAG para coletar dados de clima e enviar para GCS e BigQuery',
    schedule_interval='@hourly',
    catchup=False,
) as dag:

    # Task 1: Baixar o CSV das subprefeituras do GCS
    download_csv = GCSToLocalFilesystemOperator(
        task_id='download_csv',
        bucket='us-central1-datastorm-prod-us-a4c8a156-datafiles',
        object_name='subprefeituras-sp.csv',
        filename='/tmp/subprefeituras-sp.csv',
    )

    # Task 2: Coletar dados de clima
    collect_data_task = PythonOperator(
        task_id='collect_weather_data',
        python_callable=collect_weather_data,
        provide_context=True,
    )

    # Task 3: Enviar JSON para o Cloud Storage na pasta especificada
    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_to_gcs',
        src='/tmp/historical_weather_data.json',
        dst='LANDING_ZONE_WEATHER/historical_weather_data.json',
        bucket='us-central1-datastorm-prod-us-a4c8a156-landingzone',
    )

    # Task 4: Carregar dados do GCS para BigQuery
    load_to_bigquery = GCSToBigQueryOperator(
        task_id='load_to_bigquery',
        bucket='us-central1-datastorm-prod-us-a4c8a156-landingzone',
        source_objects=['LANDING_ZONE_WEATHER/historical_weather_data.json'],
        destination_project_dataset_table='elated-drive-432523-s4:RAW.WEATHER',  # Substitua pelo seu projeto e dataset
        source_format='NEWLINE_DELIMITED_JSON',
        write_disposition='WRITE_APPEND',
    )

    # Definir a sequência das tasks
    download_csv >> collect_data_task >> upload_to_gcs >> load_to_bigquery
