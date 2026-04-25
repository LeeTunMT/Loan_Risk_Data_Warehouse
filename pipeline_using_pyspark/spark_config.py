from pyspark.sql import SparkSession

def get_spark_session(app_name):
    """
    Production-ready SparkSession
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[2]") \
        .config("spark.driver.memory", "6g") \
        .config("spark.executor.memory", "6g") \
        .config("spark.sql.shuffle.partitions", "10") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.16") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .getOrCreate()
    
    # Temp Bucket to write BigQuery "
    spark.conf.set("temporaryGcsBucket", "gs://loan-risk-spark-temp-bucket")
    
    return spark