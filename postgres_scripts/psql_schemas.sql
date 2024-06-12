-- DROP SCHEMA IF EXISTS Ecommerce CASCADE;
-- CREATE SCHEMA Ecommerce;


DROP TABLE IF EXISTS public.olist_customers_dataset;
CREATE TABLE public.olist_customers_dataset (
    customer_id VARCHAR(40),
    customer_unique_id VARCHAR(40),
    customer_zip_code_prefix VARCHAR(5),
    customer_city VARCHAR(50),
    customer_state VARCHAR(2)
);


DROP TABLE IF EXISTS public.olist_geolocation_dataset;
CREATE TABLE public.olist_geolocation_dataset (
    geolocation_zip_code_prefix VARCHAR(5),
    geolocation_lat VARCHAR(30),
    geolocation_lng VARCHAR(30),
    geolocation_city VARCHAR(40),
    geolocation_state VARCHAR(2)
);

DROP TABLE IF EXISTS public.olist_order_items_dataset;
CREATE TABLE public.olist_order_items_dataset (
    order_id VARCHAR(40),
    order_item_id VARCHAR(2),
    product_id VARCHAR(40),
    seller_id VARCHAR(40),
    shipping_limit_date VARCHAR(20),
    price VARCHAR(10),
    freight_value VARCHAR(10)
);

DROP TABLE IF EXISTS public.olist_order_payments_dataset;
CREATE TABLE public.olist_order_payments_dataset (
    order_id VARCHAR(40),
    payment_sequential VARCHAR(2),
    payment_type VARCHAR(20),
    payment_installments VARCHAR(2),
    payment_value VARCHAR(10)
);

DROP TABLE IF EXISTS public.olist_orders_dataset;
CREATE TABLE public.olist_orders_dataset (
    order_id VARCHAR(40),
    customer_id VARCHAR(40),
    order_status VARCHAR(15),
    order_purchase_timestamp VARCHAR(20),
    order_approved_at VARCHAR(20),
    order_delivered_carrier_date VARCHAR(20),
    order_delivered_customer_date VARCHAR(20),
    order_estimated_delivery_date VARCHAR(20)
);