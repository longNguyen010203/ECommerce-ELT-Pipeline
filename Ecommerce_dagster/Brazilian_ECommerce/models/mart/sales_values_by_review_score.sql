{{ config(materialized="table") }}
{{ config(schema="mart") }}


WITH daily_sales_products AS (

  SELECT
    CAST(order_purchase_timestamp AS DATE) AS daily
    , order_id
    , SUM(CAST(payment_value AS FLOAT)) AS sales
    , COUNT(DISTINCT(order_id)) AS bills

  FROM {{ref("fact_sales")}}
  WHERE order_status = 'delivered'
  GROUP BY
    CAST(order_purchase_timestamp AS DATE)
    , order_id
), 

daily_sales_review_score AS (

  SELECT
    ts.daily
    , TO_CHAR(ts.daily, 'YYYY-MM') AS monthly
    , p.review_score AS score
    , ts.sales
    , ts.bills
    , (ts.sales / ts.bills) AS values_per_bills

  FROM daily_sales_products ts
  JOIN {{ref("dim_reviews")}} p
  ON ts.order_id = p.order_id
)

SELECT
  monthly
  , score
  , SUM(sales) AS total_sales
  , SUM(bills) AS total_bills
  , (SUM(sales) * 1.0 / SUM(bills)) AS values_per_bills

FROM daily_sales_review_score
GROUP BY
  monthly
  , score