# 🇰🇭 Cambodia Weather ETL Pipeline

Real-time ETL pipeline collecting weather data 
from all 25 provinces of Cambodia.

## 🛠️ Tech Stack
- **Apache Airflow** — Schedule & monitor pipeline
- **Apache Spark** — Distributed data transformation  
- **MySQL** — Store weather data
- **Streamlit** — 3D visualization dashboard
- **Docker** — Container orchestration

## 🚀 Quick Start

### Prerequisites
- Docker Desktop
- Python 3.8+

### Installation

1. Clone the repo
git clone https://github.com/YOUR_USERNAME/cambodia-weather-etl.git
cd cambodia-weather-etl

2. Add your API key in dags/weather_dag.py
API_KEY = "your_openweather_api_key"

3. Start Docker containers
docker-compose build
docker-compose up -d

4. Add Spark connection in Airflow
docker exec -it airflow_scheduler bash -c "airflow connections delete spark_default"
docker exec -it airflow_scheduler bash -c "airflow connections add spark_default --conn-type spark --conn-host spark-master --conn-port 7077"

5. Trigger DAG
Go to: http://localhost:8081
Login: admin / admin
Trigger: cambodia_weather_etl

6. Run Dashboard
pip install streamlit pandas pydeck mysql-connector-python sqlalchemy
streamlit run app.py
Go to: http://localhost:8501

## 📊 Pipeline Flow
[OpenWeather API] → Extract → Transform (Spark) → Load (MySQL) → Streamlit Dashboard

## 🌡️ Dashboard Features
- 3D Temperature Map
- Province Rankings
- Heat Level Classification
- Real-time Updates (hourly)