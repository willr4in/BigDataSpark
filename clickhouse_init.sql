CREATE DATABASE IF NOT EXISTS bigdata;

-- Покупатели
CREATE TABLE IF NOT EXISTS bigdata.dim_customers
(
    customer_id UInt32,
    first_name String,
    last_name String,
    age UInt8,
    email String,
    country String,
    postal_code String
) ENGINE = MergeTree
ORDER BY customer_id;

-- Домашние животные
CREATE TABLE IF NOT EXISTS bigdata.dim_pets
(
    pet_id UInt32,
    type String,
    name String,
    breed String,
    category String,
    owner_id UInt32
) ENGINE = MergeTree
ORDER BY pet_id;

-- Продавцы
CREATE TABLE IF NOT EXISTS bigdata.dim_sellers
(
    seller_id UInt32,
    first_name String,
    last_name String,
    email String,
    country String,
    postal_code String
) ENGINE = MergeTree
ORDER BY seller_id;

-- Товары
CREATE TABLE IF NOT EXISTS bigdata.dim_products
(
    product_id UInt32,
    name String,
    category String,
    price Float64,
    weight Float64,
    color String,
    size String,
    brand String,
    material String,
    description String,
    rating Float64,
    reviews UInt32,
    release_date Date,
    expiry_date Date
) ENGINE = MergeTree
ORDER BY product_id;

-- Поставщики
CREATE TABLE IF NOT EXISTS bigdata.dim_suppliers
(
    supplier_id UInt32,
    name String,
    contact String,
    email String,
    phone String,
    address String,
    city String,
    country String
) ENGINE = MergeTree
ORDER BY supplier_id;

-- Магазины
CREATE TABLE IF NOT EXISTS bigdata.dim_stores
(
    store_id UInt32,
    name String,
    location String,
    city String,
    state String,
    country String,
    phone String,
    email String
) ENGINE = MergeTree
ORDER BY store_id;

-- Даты
CREATE TABLE IF NOT EXISTS bigdata.dim_dates
(
    date Date,
    year UInt16,
    month UInt8,
    day UInt8,
    week UInt8,
    quarter UInt8,
    weekday UInt8
) ENGINE = MergeTree
ORDER BY date;

-- Факт продаж
CREATE TABLE IF NOT EXISTS bigdata.fact_sales
(
    sale_id UInt32,
    sale_date Date,
    customer_id UInt32,
    pet_id UInt32,
    seller_id UInt32,
    product_id UInt32,
    store_id UInt32,
    supplier_id UInt32,
    quantity UInt32,
    total_price Float64
) ENGINE = MergeTree
ORDER BY sale_id;
