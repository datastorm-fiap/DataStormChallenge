from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    dag_id='etl_raw_to_trusted_weather',
    default_args=default_args,
    description='Pipeline to process WEATHER and SUBPREFECTURES tables from RAW to TRUSTED layer',
    schedule_interval='@hourly',
    catchup=False,
) as dag:

    # Step 1: Create VIEW for SUBPREFECTURES in TRUSTED directly from RAW
    process_subprefectures = BigQueryExecuteQueryOperator(
        task_id='process_subprefectures',
        sql="""
            CREATE OR REPLACE VIEW `elated-drive-432523-s4.TRUSTED.SUBPREFECTURES` AS
            SELECT 
                REPLACE(UPPER(SUBSTRING(Subprefeitura, 1, 1)) || LOWER(SUBSTRING(Subprefeitura, 2)), ' ', '') AS SubprefectureName,
                ROUND(Latitude, 2) AS Latitude,  -- Rounding to 2 decimal places
                ROUND(Longitude, 2) AS Longitude,  -- Rounding to 2 decimal places
                CAST(REPLACE(CEP, '-', '') AS INT64) AS PostalCode
            FROM `elated-drive-432523-s4.RAW.SUBPREFECTURES`
        """,
        use_legacy_sql=False
    )

    # Step 2: Create VIEW for WEATHER in TRUSTED directly from RAW
    process_weather = BigQueryExecuteQueryOperator(
        task_id='process_weather',
        sql="""
            CREATE OR REPLACE VIEW `elated-drive-432523-s4.TRUSTED.WEATHER_API` AS
            SELECT 
                REPLACE(UPPER(SUBSTRING(subprefeitura, 1, 1)) || LOWER(SUBSTRING(subprefeitura, 2)), ' ', '') AS SubprefectureName,
                ROUND(Latitude, 2) AS Latitude,  -- Rounding to 2 decimal places
                ROUND(Longitude, 2) AS Longitude,  -- Rounding to 2 decimal places
                TIMESTAMP(timestamp) AS ObservationTime,
                temperature AS Temperature,
                feels_like AS FeelsLike,
                pressure AS Pressure,
                humidity AS Humidity,
                temp_min AS MinTemperature,
                temp_max AS MaxTemperature,
                wind_speed AS WindSpeed,
                wind_deg AS WindDirection,
                wind_gust AS WindGust,
                clouds_all AS CloudCoverage,
                weather_id AS WeatherID,
                weather_main AS WeatherMain,
                weather_description AS WeatherDescription,
                weather_icon AS WeatherIcon
            FROM `elated-drive-432523-s4.RAW.WEATHER`
        """,
        use_legacy_sql=False
    )

    # Step 3: Create VIEW to Join WEATHER and SUBPREFECTURES in TRUSTED layer
    create_trusted_weather = BigQueryExecuteQueryOperator(
        task_id='create_trusted_weather',
        sql="""
            CREATE OR REPLACE VIEW `elated-drive-432523-s4.TRUSTED.WEATHER` AS
            SELECT 
                w.*,
                s.PostalCode,
                s.SubprefectureName AS SubprefectureNameTrusted
            FROM `elated-drive-432523-s4.TRUSTED.WEATHER_API` w
            LEFT JOIN `elated-drive-432523-s4.TRUSTED.SUBPREFECTURES` s
            ON ROUND(w.Latitude, 2) = ROUND(s.Latitude, 2)  -- Ensuring precision alignment
            AND ROUND(w.Longitude, 2) = ROUND(s.Longitude, 2)  -- Ensuring precision alignment
        """,
        use_legacy_sql=False
    )

    # Define Task Dependencies
    process_subprefectures >> process_weather >> create_trusted_weather 
