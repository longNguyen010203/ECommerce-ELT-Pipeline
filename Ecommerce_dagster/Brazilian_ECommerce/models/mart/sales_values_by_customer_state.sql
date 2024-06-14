{{ config(materialized="table") }}
{{ config(schema="mart") }}


WITH daily_sales_customers AS (
  
  SELECT
    CAST(order_purchase_timestamp AS DATE) AS daily
    , customer_id
    , SUM(CAST(payment_value AS FLOAT)) AS sales
    , COUNT(DISTINCT(order_id)) AS bills

  FROM {{ref("fact_sales")}}
  WHERE order_status = 'delivered'
  GROUP BY
    CAST(order_purchase_timestamp AS DATE)
    , customer_id
), 

daily_sales_customer_status AS (

  SELECT
    ts.daily
    , TO_CHAR(ts.daily, 'YYYY-MM') AS monthly
    , p.customer_state AS state
    , ts.sales
    , ts.bills
    , (ts.sales / ts.bills) AS values_per_bills
    
  FROM daily_sales_customers ts
  JOIN {{ref("dim_customers")}} p
  ON ts.customer_id = p.customer_id
)

SELECT
  monthly
  , state
  , SUM(sales) AS total_sales
  , SUM(bills) AS total_bills
  , (SUM(sales) * 1.0 / SUM(bills)) AS values_per_bills
FROM daily_sales_customer_status
GROUP BY
  monthly
  , state