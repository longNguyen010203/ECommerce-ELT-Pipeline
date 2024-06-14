{{ config(materialized="table") }}
{{ config(schema="mart") }}


WITH daily_sales_geolocation_city AS (

    SELECT
        ts.daily
        , TO_CHAR(ts.daily, 'YYYY-MM') AS monthly
        , geolocation_city AS city
        , ts.sales
        , ts.bills
        , (ts.sales / ts.bills) AS values_per_bills

    FROM (
        SELECT
            CAST(do.order_purchase_timestamp AS DATE) AS daily
            , do.order_id
            , geolocation_city
            , SUM(CAST(do.payment_value AS FLOAT)) AS sales
            , COUNT(DISTINCT(do.order_id)) AS bills

        FROM {{ ref("dim_geolocation") }} dg 
            INNER JOIN {{ ref("dim_customers") }} dc 
            ON dg.geolocation_zip_code_prefix = dc.customer_zip_code_prefix
            INNER JOIN {{ ref("dim_orders") }} do
            ON dc.customer_id = do.customer_id

        WHERE order_status = 'delivered'
        GROUP BY
            CAST(order_purchase_timestamp AS DATE)
            , order_id
            , geolocation_city
    ) AS ts
    
)

SELECT
  monthly
  , city
  , SUM(sales) AS total_sales
  , SUM(bills) AS total_bills
  , (SUM(sales) * 1.0 / SUM(bills)) AS values_per_bills

FROM daily_sales_geolocation_city
GROUP BY
  monthly
  , city