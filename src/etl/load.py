import os
import pandas as pd
from sqlalchemy import create_engine
import boto3
import tempfile

# -----------------------------------
# FUNCIONES PARA RAW DATA
# -----------------------------------
def save_raw_local(df, path="data/raw/shows_raw.csv"):
    """Guarda el dataset raw localmente"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    print(f"Raw data guardado localmente en: {path}")


def save_raw_s3(df, bucket_name, s3_key,
                aws_access_key_id=None, aws_secret_access_key=None, region_name="us-east-1"):
    """Sube el dataset raw a S3"""
    # Guardar en archivo temporal
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
        df.to_csv(tmp.name, index=False)
        tmp_path = tmp.name

    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name
    )
    s3.upload_file(tmp_path, bucket_name, s3_key)
    os.remove(tmp_path)
    print(f"Raw data subido a S3: s3://{bucket_name}/{s3_key}")


# -----------------------------------
# FUNCIONES PARA PROCESSED DATA
# -----------------------------------
def save_processed_local(df, path="data/processed/shows_clean.csv"):
    """Guarda el dataset procesado localmente"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    print(f"Processed data guardado localmente en: {path}")


def save_processed_s3(df, bucket_name, s3_key,
                      aws_access_key_id=None, aws_secret_access_key=None, region_name="us-east-1"):
    """Sube el dataset procesado a S3"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
        df.to_csv(tmp.name, index=False)
        tmp_path = tmp.name

    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name
    )
    s3.upload_file(tmp_path, bucket_name, s3_key)
    os.remove(tmp_path)
    print(f"Processed data subido a S3: s3://{bucket_name}/{s3_key}")


def save_processed_postgres(df, db_url, table_name="shows"):
    """
    Guarda el dataset procesado en PostgreSQL.
    Convierte listas y diccionarios a string para evitar errores.
    """
    df_to_save = df.copy()

    for col in df_to_save.columns:
        if df_to_save[col].apply(lambda x: isinstance(x, (list, dict))).any():
            df_to_save[col] = df_to_save[col].astype(str)

    engine = create_engine(db_url)
    df_to_save.to_sql(table_name, engine, if_exists="replace", index=False)
    print(f"Processed data cargado a Postgres en la tabla '{table_name}' ({len(df_to_save)} filas)")
