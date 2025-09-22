import os
from dotenv import load_dotenv
from extract import extract
from transform import transform_data
from load import (
    save_raw_local,
    save_raw_s3,
    save_processed_local,
    save_processed_s3,
    save_processed_postgres
)

# Cargar variables de entorno
load_dotenv()

# Credenciales S3
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

# Credenciales Postgres
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")

DB_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# Base directory del script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


if __name__ == "__main__":
    # === 1. Extraer ===
    df_raw = extract()
    print("Raw data preview:\n", df_raw.head())

    # === 2. Transformar ===
    df_clean = transform_data(df_raw)

    # === 3. Cargar ===
    # --- Guardar dataset raw ---
    raw_local_path = os.path.join(BASE_DIR, "../../data/raw/shows_raw.csv")
    save_raw_local(df_raw, raw_local_path)

    # save_raw_s3(
    #     df_raw,
    #     bucket_name=S3_BUCKET_NAME,
    #     s3_key="raw/shows_raw.csv",
    #     aws_access_key_id=AWS_ACCESS_KEY_ID,
    #     aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    #     region_name=AWS_REGION
    # )

    # --- Guardar dataset procesado ---
    processed_local_path = os.path.join(BASE_DIR, "../../data/processed/shows_clean.csv")
    save_processed_local(df_clean, processed_local_path)

    # save_processed_s3(
    #     df_clean,
    #     bucket_name=S3_BUCKET_NAME,
    #     s3_key="processed/shows_clean.csv",
    #     aws_access_key_id=AWS_ACCESS_KEY_ID,
    #     aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    #     region_name=AWS_REGION
    # )

    save_processed_postgres(df_clean, db_url=DB_URL, table_name="shows")
