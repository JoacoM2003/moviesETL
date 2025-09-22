# 🎬 Movies ETL Pipeline

**Movies ETL Pipeline** es un pipeline ETL de películas que extrae, transforma y carga datos usando Python y Pandas. Automatización completa con Apache Airflow, almacenamiento en CSV y PostgreSQL, integración opcional con S3, y exposición de los datos mediante una API.

## 🛠️ Tecnologías

- Python
- Pandas
- PostgreSQL
- Apache Airflow
- Docker
- FastAPI (para exposición de datos)

## 🚀 Funcionalidades

- Extracción de datos desde archivos locales (y potencialmente S3)
- Transformación y limpieza de datos
- Carga en CSV local, S3 y base de datos PostgreSQL
- Automatización de tareas mediante Airflow DAGs
- Exposición de datos mediante API para consultas y acceso programático
- Registro de logs y resultados de cada ejecución

## 🚀 Instalación

1. Clonar el repositorio:
git clone https://github.com/usuario/movies-etl.git
cd movies-etl

2. Crear un archivo `.env` con las variables necesarias:
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=us-east-1
S3_BUCKET_NAME=...

POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=airflow

3. Construir y levantar los contenedores:
docker-compose up -d

4. Inicializar la base de datos de Airflow:
docker-compose exec airflow airflow db init

5. Crear un usuario admin:
docker-compose exec airflow airflow users create --username admin --firstname Admin --lastname Admin --role Admin --email admin@example.com

6. Abrir la interfaz de Airflow en el navegador:
http://localhost:8080

## 🧪 Uso

- DAG principal: `movies_etl_pandas`. Automatiza la ejecución del ETL en horarios programados (@daily por defecto)
- Logs y resultados disponibles en `airflow/logs/` y `data/processed/`
- CSV y PostgreSQL son actualizados automáticamente en cada ejecución

Ejecutar manualmente:
docker-compose exec airflow airflow dags trigger movies_etl_pandas

**API para exposición de datos:**
- Permite consultar los datos procesados en tiempo real
- Endpoints disponibles para acceder a películas, métricas y KPIs
- Puede correr dentro del mismo contenedor o en un servicio separado

## 🎬 Demo

Puedes ver una demo del proyecto en funcionamiento en el siguiente enlace:
En construcción