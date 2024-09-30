# DataStorm: Predictive Analytics for Power Outages

## Project Overview
DataStorm is a predictive analytics solution designed to support energy companies by forecasting the likelihood of power outages in São Paulo based on near real-time weather data and historical occurrences. Developed as part of the FIAP and Minsait 2024 Challenge, this project aims to enhance operational efficiency and support proactive emergency management.

## Problem Statement
The absence of real-time meteorological data integration is a significant issue for energy companies. This lack of data impacts the efficiency of operations, resource allocation, and response to adverse weather events, increasing the risk of service interruptions. The DataStorm solution aims to fill this gap by integrating multiple data sources and leveraging machine learning models to provide accurate predictions and actionable insights.

## Project Objectives
- **Data Lake Development**: Create a robust data lake infrastructure to store and process real-time and historical data, including weather conditions, power outages, and demographic information.
- **Predictive Modeling**: Develop machine learning models to predict power outage probabilities in different regions of São Paulo based on the collected data.
- **Data Integration**: Streamline data ingestion and transformation pipelines for real-time and historical data from various public APIs and sources.
- **Dashboard Visualization**: Provide interactive dashboards and visualizations for stakeholders to monitor predictions and prioritize resource allocation.

## Solution Architecture
The solution is built using the Google Cloud Platform, integrating multiple data sources via an ETL pipeline orchestrated with Apache Airflow:

1. **ETL Pipeline**:  
   - **RAW Layer**: Ingests real-time and historical data into the system with minimal transformations.
   - **TRUSTED Layer**: Processes and filters data into structured formats suitable for modeling.
   - **REFINED Layer**: Combines all data into a unified format for analysis and machine learning applications.
   
2. **Machine Learning Models**:  
   - Trained using historical data from 2022 to 2024 to predict the likelihood of power outages.  
   - Models tested include Decision Trees, Random Forest, Gradient Boosting, and LightGBM. The chosen model is LightGBM due to its high accuracy and efficiency.

3. **Dashboard and Visualization**:  
   - Visualizations are developed using Power BI, allowing stakeholders to view predictive analytics, subprefecture rankings, and real-time weather alerts.

## Data Sources
The data lake integrates the following sources:
- **Near Real-Time Weather Data**: API for hourly weather updates and 7-day forecasts, including precipitation, temperature, and humidity.
- **Historical Data**: Daily weather records, power outage occurrences, and emergency incidents from 2022 to 2024.
- **Demographic Data**: Population density, number of hospitals, number of favelas, and Human Development Index (HDI) for ranking subprefectures based on criticality.

## Key Features
- **Real-Time Data Integration**: Continuous data ingestion from public APIs to ensure up-to-date insights.
- **Predictive Analytics**: Machine learning models provide accurate forecasts for potential power outages.
- **Decision Support System**: Interactive dashboards enable informed decision-making and prioritization of emergency responses.

## How It Works
The solution follows these steps:
1. **Data Ingestion**: Raw data is ingested from multiple sources using the ETL pipelines and stored in the RAW layer.
2. **Data Transformation**: Data is cleaned, processed, and stored in the TRUSTED layer.
3. **Data Integration**: The REFINED layer consolidates all data into a single view for analysis.
4. **Model Training**: Historical data is used to train the machine learning models.
5. **Prediction**: Trained models predict future power outages based on near real-time weather data.
6. **Visualization**: Results are visualized in dashboards for easy interpretation and decision-making.

## Project Structure
The repository is organized as follows:

```plaintext
├── DAGs/
│   ├── 1. ETL_1 (Drive para RAW no BigQuery).ipynb
│   ├── 2. ETL_2 RAW to TRUSTED.ipynb
│   ├── 3. ETL_3 TRUSTED to REFINED.ipynb
│   ├── dags_etl_raw_to_trusted_weather.py
│   ├── dags_etl_trusted_to_refined_weather.py
│   ├── dags_weather_data_pipeline_dag.py
│   └── ArchitectureNOSQL-Sprint3Doc.pdf
├── Models/
│   ├── 4. Teste Modelos Preditivos.ipynb
│   └── 5. Predição e DF Final.ipynb
├── Documentation/
│   ├── BIGData&Architecture-Sprint3Doc.pdf
│   └── DataStorm_Sprint4_Presentation.pdf
└── README.md
