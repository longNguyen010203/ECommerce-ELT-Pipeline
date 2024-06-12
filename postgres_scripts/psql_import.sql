\copy olist_customers_dataset(customer_id,customer_unique_id,customer_zip_code_prefix,customer_city,customer_state) FROM 'tmp/brazilian/olist_customers_dataset.csv' DELIMITER ',' CSV HEADER;

\copy olist_geolocation_dataset(geolocation_zip_code_prefix,geolocation_lat,geolocation_lng,geolocation_city,geolocation_state) FROM 'tmp/brazilian/olist_geolocation_dataset.csv' DELIMITER ',' CSV HEADER;

\copy olist_order_items_dataset(order_id,order_item_id,product_id,seller_id,shipping_limit_date,price,freight_value) FROM 'tmp/brazilian/olist_order_items_dataset.csv' DELIMITER ',' CSV HEADER;

\copy olist_order_payments_dataset(order_id,payment_sequential,payment_type,payment_installments,payment_value) FROM 'tmp/brazilian/olist_order_payments_dataset.csv' DELIMITER ',' CSV HEADER;

\copy olist_orders_dataset(order_id,customer_id,order_status,order_purchase_timestamp,order_approved_at,order_delivered_carrier_date,order_delivered_customer_date,order_estimated_delivery_date) FROM 'tmp/brazilian/olist_orders_dataset.csv' DELIMITER ',' CSV HEADER;