def save_parquet(df, output_path):
    df.write.mode("overwrite").parquet(output_path)
    print(f"Datos guardados en {output_path} (parquet)")

def save_csv(df, output_path):
    df.write.mode("overwrite").option("header", "true").csv(output_path)
    print(f"Datos guardados en {output_path} (csv)")

def save_postgres(df, url, table, user, password):
    df.write \
      .format("jdbc") \
      .option("url", url) \
      .option("dbtable", table) \
      .option("user", user) \
      .option("password", password) \
      .mode("overwrite") \
      .save()
    print(f"Datos cargados en Postgres: {table}")