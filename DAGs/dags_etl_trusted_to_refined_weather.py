from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    dag_id='etl_trusted_to_refined_weather',
    default_args=default_args,
    description='Pipeline to move WEATHER data from TRUSTED to REFINED layer and add descriptions',
    schedule_interval='@hourly',
    catchup=False,
) as dag:

    # Step 1: Create TABLE for WEATHER in REFINED layer from the TRUSTED view
    create_refined_weather_table = BigQueryExecuteQueryOperator(
        task_id='create_refined_weather_table',
        sql="""
            CREATE OR REPLACE TABLE `elated-drive-432523-s4.REFINED.WEATHER` AS
            SELECT * FROM `elated-drive-432523-s4.TRUSTED.WEATHER`;
        """,
        use_legacy_sql=False
    )

    # Step 2: Consolidated ALTER TABLE to Add Descriptions to REFINED.WEATHER Table
    add_descriptions_to_refined_weather = BigQueryExecuteQueryOperator(
        task_id='add_descriptions_to_refined_weather',
        sql="""
            ALTER TABLE `elated-drive-432523-s4.REFINED.WEATHER`
            SET OPTIONS (
              description = 'Refined table with enriched weather and subprefecture data'
            );

            ALTER TABLE `elated-drive-432523-s4.REFINED.WEATHER`
            ALTER COLUMN SubprefectureName SET OPTIONS (description = 'Standardized name of the subprefecture from the Open Weather API'),
            ALTER COLUMN Latitude SET OPTIONS (description = 'Subprefecture Latitude'),
            ALTER COLUMN Longitude SET OPTIONS (description = 'Subprefecture Longitude'),
            ALTER COLUMN ObservationTime SET OPTIONS (description = 'Datatime of the weather conditions obsevation'),
            ALTER COLUMN Temperature SET OPTIONS (description = 'Recorded temperature in Kelvin'),
            ALTER COLUMN FeelsLike SET OPTIONS (description = 'Apparent temperature (feels like) in Kelvin'),
            ALTER COLUMN Pressure SET OPTIONS (description = 'Atmospheric pressure in hectopascals (hPa)'),
            ALTER COLUMN Humidity SET OPTIONS (description = 'Relative humidity percentage'),
            ALTER COLUMN MinTemperature SET OPTIONS (description = 'Minimum temperature recorded in Kelvin'),
            ALTER COLUMN MaxTemperature SET OPTIONS (description = 'Maximum temperature recorded in Kelvin'),
            ALTER COLUMN WindSpeed SET OPTIONS (description = 'Wind speed in meters per second'),
            ALTER COLUMN WindDirection SET OPTIONS (description = 'Wind direction in degrees'),
            ALTER COLUMN WindGust SET OPTIONS (description = 'Wind gust speed in meters per second'),
            ALTER COLUMN CloudCoverage SET OPTIONS (description = 'Percentage of cloud cover'),
            ALTER COLUMN WeatherID SET OPTIONS (description = 'Identifier for the weather type'),
            ALTER COLUMN WeatherMain SET OPTIONS (description = 'Main category of the weather (e.g., Clouds, Rain)'),
            ALTER COLUMN WeatherDescription SET OPTIONS (description = 'Detailed description of the weather conditions'),
            ALTER COLUMN WeatherIcon SET OPTIONS (description = 'Code representing the weather icon'),
            ALTER COLUMN PostalCode SET OPTIONS (description = 'Postal code of the subprefecture'),
            ALTER COLUMN SubprefectureNameTrusted SET OPTIONS (description = 'Standardized name of the subprefecture from the SUBPREFECTURES Description of SP Prefecture');
        """,
        use_legacy_sql=False
    )

    # Define Task Dependencies
    create_refined_weather_table >> add_descriptions_to_refined_weather
