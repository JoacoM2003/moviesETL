from reader import create_spark, read_data
from transformer import transform_data
from writer import save_parquet, save_postgres

if __name__ == "__main__":
    spark = create_spark()

    # === Extract ===
    local_path = "../../data/raw/shows_raw.csv"
    df_raw = read_data(spark, source="local", path=local_path)

    # === Transform ===
    df_transformed = transform_data(df_raw)

    # === Load ===
    save_parquet(df_transformed, "../../data/processed/shows_spark.parquet")

    # Postgres (opcional)
    save_postgres(df_transformed,
                  url="jdbc:postgresql://localhost:5432/movies_db",
                  table="shows_spark",
                  user="postgres",
                  password="admin")