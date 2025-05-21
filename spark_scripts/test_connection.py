from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TestConnection") \
    .config("spark.jars", "/app/jars/postgresql-42.7.5.jar") \
    .getOrCreate()

url = "jdbc:postgresql://postgres:5432/BigDatalab2"
properties = {"user": "user", "password": "1234", "driver": "org.postgresql.Driver"}

try:
    df = spark.read.jdbc(url=url, table="mock_data", properties=properties)
    df.show(5)
    print("Connection successful!")
except Exception as e:
    print("Connection failed:", e)
