from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id

# Настройка Spark с драйвером PostgreSQL
spark = SparkSession.builder \
    .appName("SnowflakeModel") \
    .config("spark.jars", "jars/postgresql-42.7.5.jar") \
    .getOrCreate()

# Параметры подключения
url = "jdbc:postgresql://postgres:5432/BigDatalab2"
properties = {
    "user": "user",
    "password": "1234",
    "driver": "org.postgresql.Driver"
}

# Загрузка исходной таблицы
mock_df = spark.read.jdbc(url=url, table="mock_data", properties=properties)

# === Измерения ===

# dim_customers
customers = mock_df.select(
    col("sale_customer_id").alias("customer_id"),
    "customer_first_name", "customer_last_name", "customer_age",
    "customer_email", "customer_country", "customer_postal_code"
).dropDuplicates(["customer_id"])

# dim_pets
pets = mock_df.select(
    monotonically_increasing_id().alias("pet_id"),
    col("sale_customer_id").alias("customer_id"),
    col("customer_pet_type").alias("pet_type"),
    col("customer_pet_name").alias("pet_name"),
    col("customer_pet_breed").alias("pet_breed"),
    col("pet_category")
).dropDuplicates(["customer_id", "pet_name", "pet_breed"])

# dim_sellers
sellers = mock_df.select(
    col("sale_seller_id").alias("seller_id"),
    "seller_first_name", "seller_last_name", "seller_email",
    "seller_country", "seller_postal_code"
).dropDuplicates(["seller_id"])

# dim_products
products = mock_df.select(
    col("sale_product_id").alias("product_id"),
    "product_name", "product_category", "product_price", "product_quantity",
    "product_weight", "product_color", "product_size", "product_brand",
    "product_material", "product_description", "product_rating",
    "product_reviews", "product_release_date", "product_expiry_date"
).dropDuplicates(["product_id"])

# dim_suppliers
suppliers = mock_df.select(
    "supplier_name", "supplier_contact", "supplier_email", "supplier_phone",
    "supplier_address", "supplier_city", "supplier_country"
).dropDuplicates()

# dim_stores
stores = mock_df.select(
    "store_name", "store_location", "store_city", "store_state", "store_country",
    "store_phone", "store_email"
).dropDuplicates()

# dim_dates (из sale_date)
dates = mock_df.select(col("sale_date").alias("date")).dropDuplicates()

# === Фактовая таблица ===
facts = mock_df.select(
    col("id").alias("sale_id"),
    col("sale_date"),
    col("sale_customer_id").alias("customer_id"),
    col("sale_seller_id").alias("seller_id"),
    col("sale_product_id").alias("product_id"),
    col("sale_quantity"),
    col("sale_total_price")
)

# === Запись в PostgreSQL ===
customers.write.jdbc(url, "dim_customers", mode="overwrite", properties=properties)
pets.write.jdbc(url, "dim_pets", mode="overwrite", properties=properties)
sellers.write.jdbc(url, "dim_sellers", mode="overwrite", properties=properties)
products.write.jdbc(url, "dim_products", mode="overwrite", properties=properties)
suppliers.write.jdbc(url, "dim_suppliers", mode="overwrite", properties=properties)
stores.write.jdbc(url, "dim_stores", mode="overwrite", properties=properties)
dates.write.jdbc(url, "dim_dates", mode="overwrite", properties=properties)
facts.write.jdbc(url, "fact_sales", mode="overwrite", properties=properties)

spark.stop()
