from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg, month, year

# Настройка Spark с драйверами PostgreSQL и ClickHouse
spark = SparkSession.builder \
    .appName("Reports") \
    .config("spark.jars", "jars/postgresql-42.7.5.jar,jars/clickhouse-jdbc-0.4.6-shaded.jar") \
    .getOrCreate()

# Параметры подключения к PostgreSQL
pg_url = "jdbc:postgresql://postgres:5432/BigDatalab2"
pg_properties = {
    "user": "user",
    "password": "1234",
    "driver": "org.postgresql.Driver"
}

# Параметры подключения к ClickHouse
ch_url = "jdbc:clickhouse://clickhouse:8123/bigdata"
ch_properties = {
    "driver": "com.clickhouse.jdbc.ClickHouseDriver"
}

# Загрузка данных из PostgreSQL
customers_df = spark.read.jdbc(pg_url, "dim_customers", properties=pg_properties)
products_df = spark.read.jdbc(pg_url, "dim_products", properties=pg_properties)
sales_df = spark.read.jdbc(pg_url, "fact_sales", properties=pg_properties)
stores_df = spark.read.jdbc(pg_url, "dim_stores", properties=pg_properties)
suppliers_df = spark.read.jdbc(pg_url, "dim_suppliers", properties=pg_properties)

# Витрина продаж по продуктам
top_products = sales_df.groupBy("product_id").agg(
    sum("sale_quantity").alias("total_quantity"), 
    sum("sale_total_price").alias("total_revenue")  
).join(products_df, "product_id") \
  .orderBy(col("total_quantity").desc()) \
  .limit(10)

top_products.write.jdbc(ch_url, "top_products", mode="overwrite", properties=ch_properties)

# Общая выручка по категориям продуктов
revenue_by_category = sales_df.groupBy("product_id").agg(
    sum("sale_total_price").alias("total_revenue")  
).join(products_df, "product_id") \
  .groupBy("product_category").agg(
      sum("total_revenue").alias("total_revenue")
  )

revenue_by_category.write.jdbc(ch_url, "revenue_by_category", mode="overwrite", properties=ch_properties)

# Средний рейтинг и количество отзывов для каждого продукта
avg_rating_reviews = products_df.groupBy("product_id").agg(
    avg("product_rating").alias("avg_rating"),
    sum("product_reviews").alias("total_reviews")
)

avg_rating_reviews.write.jdbc(ch_url, "avg_rating_reviews", mode="overwrite", properties=ch_properties)

# Витрина продаж по клиентам
top_customers = sales_df.groupBy("customer_id").agg(
    sum("sale_total_price").alias("total_spent")  
).orderBy(col("total_spent").desc()).limit(10)

top_customers.write.jdbc(ch_url, "top_customers", mode="overwrite", properties=ch_properties)

# Распределение клиентов по странам
customers_by_country = customers_df.groupBy("customer_country").agg(
    count("customer_id").alias("customer_count")
)

customers_by_country.write.jdbc(ch_url, "customers_by_country", mode="overwrite", properties=ch_properties)

# Средний чек для каждого клиента
avg_check_per_customer = sales_df.groupBy("customer_id").agg(
    avg("sale_total_price").alias("avg_check") 
)

avg_check_per_customer.write.jdbc(ch_url, "avg_check_per_customer", mode="overwrite", properties=ch_properties)

# Витрина продаж по времени
monthly_trends = sales_df.groupBy(year("sale_date").alias("year"), month("sale_date").alias("month")).agg(
    sum("sale_total_price").alias("total_revenue")  
)

monthly_trends.write.jdbc(ch_url, "monthly_trends", mode="overwrite", properties=ch_properties)

# Средний размер заказа по месяцам
avg_order_size_per_month = sales_df.groupBy(year("sale_date").alias("year"), month("sale_date").alias("month")).agg(
    avg("sale_quantity").alias("avg_order_size")  
)

avg_order_size_per_month.write.jdbc(ch_url, "avg_order_size_per_month", mode="overwrite", properties=ch_properties)

# Витрина продаж по магазинам
top_stores = sales_df.groupBy("store_id").agg(
    sum("sale_total_price").alias("total_revenue")  
).join(stores_df, "store_id") \
  .orderBy(col("total_revenue").desc()).limit(5)

top_stores.write.jdbc(ch_url, "top_stores", mode="overwrite", properties=ch_properties)

# Распределение продаж по городам и странам
sales_by_location = sales_df.groupBy("store_id").agg(
    sum("sale_total_price").alias("total_revenue")  
).join(stores_df, "store_id") \
  .groupBy("store_city", "store_country").agg(
      sum("total_revenue").alias("total_revenue")
  )

sales_by_location.write.jdbc(ch_url, "sales_by_location", mode="overwrite", properties=ch_properties)

# Средний чек для каждого магазина
avg_check_per_store = sales_df.groupBy("store_id").agg(
    avg("sale_total_price").alias("avg_check")  
)

avg_check_per_store.write.jdbc(ch_url, "avg_check_per_store", mode="overwrite", properties=ch_properties)

# Витрина продаж по поставщикам
top_suppliers = sales_df.groupBy("supplier_id").agg(
    sum("sale_total_price").alias("total_revenue")  
).join(suppliers_df, "supplier_id") \
  .orderBy(col("total_revenue").desc()).limit(5)

top_suppliers.write.jdbc(ch_url, "top_suppliers", mode="overwrite", properties=ch_properties)

# Средняя цена товаров от каждого поставщика
avg_price_per_supplier = products_df.groupBy("supplier_id").agg(
    avg("product_price").alias("avg_price")
)

avg_price_per_supplier.write.jdbc(ch_url, "avg_price_per_supplier", mode="overwrite", properties=ch_properties)

# Распределение продаж по странам поставщиков
sales_by_supplier_country = sales_df.groupBy("supplier_id").agg(
    sum("sale_total_price").alias("total_revenue") 
).join(suppliers_df, "supplier_id") \
  .groupBy("supplier_country").agg(
      sum("total_revenue").alias("total_revenue")
  )

sales_by_supplier_country.write.jdbc(ch_url, "sales_by_supplier_country", mode="overwrite", properties=ch_properties)

# Витрина качества продукции
highest_lowest_rating = products_df.select(
    col("product_id"),
    col("product_rating")
).orderBy(col("product_rating").asc()).limit(1).union(
    products_df.select(
        col("product_id"),
        col("product_rating")
    ).orderBy(col("product_rating").desc()).limit(1)
)

highest_lowest_rating.write.jdbc(ch_url, "highest_lowest_rating", mode="overwrite", properties=ch_properties)

# Корреляция между рейтингом и объемом продаж
correlation_rating_sales = sales_df.join(products_df, "product_id").groupBy("product_id").agg(
    sum("sale_quantity").alias("total_quantity"),  
    avg("product_rating").alias("avg_rating")
)

correlation_rating_sales.write.jdbc(ch_url, "correlation_rating_sales", mode="overwrite", properties=ch_properties)

# Продукты с наибольшим количеством отзывов
most_reviewed_products = products_df.orderBy(col("product_reviews").desc()).limit(10)

most_reviewed_products.write.jdbc(ch_url, "most_reviewed_products", mode="overwrite", properties=ch_properties)

spark.stop()
