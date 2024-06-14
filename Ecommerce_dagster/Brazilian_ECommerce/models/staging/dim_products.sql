{{ config(materialized="table") }}
{{ config(schema="staging") }}


SELECT
    opd.product_id
    , pcnt.product_category_name_english

FROM {{ source("raw", "raw_products") }} opd JOIN 
     {{ source("raw", "raw_product_category_name_translation") }} pcnt
    ON opd.product_category_name = pcnt.product_category_name