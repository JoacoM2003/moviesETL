from pyspark.sql.functions import col, when, initcap, to_date

def transform_data(df):
    df = df.select("id", "name", "genres", "status", "language", "premiered", "network", "rating")

    # Convertir tipos básicos
    df = df.withColumn("premiered", to_date("premiered", "yyyy-MM-dd"))
    df = df.withColumn("rating", col("rating").cast("float"))

    # Normalizar texto
    df = df.withColumn("status", initcap(col("status")))
    df = df.withColumn("language", initcap(col("language")))

    # Network -> "Unknown" si es nulo
    df = df.withColumn("network", when(col("network").isNull(), "Unknown").otherwise(col("network")))

    # Filtrar filas válidas (ejemplo: con id y name no nulos)
    df = df.filter(col("id").isNotNull() & col("name").isNotNull())

    # Eliminar duplicados por id
    df = df.dropDuplicates(["id"])

    print(f"Transformación completada: {df.count()} filas procesadas")
    return df
