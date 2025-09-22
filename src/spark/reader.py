from pyspark.sql import SparkSession

def create_spark():
    return (
        SparkSession.builder
        .appName("MoviesETL")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
        .getOrCreate()
    )

def read_data(spark, source="local", path=None):
    if source not in ["local", "s3"]:
        raise ValueError("source debe ser 'local' o 's3'")
    
    df = spark.read.csv(path, header=True, inferSchema=True)
    print(f"Datos leídos desde {source}: {df.count()} filas")
    return df
