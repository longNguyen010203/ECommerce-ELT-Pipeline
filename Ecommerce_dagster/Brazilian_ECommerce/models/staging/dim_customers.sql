{{ config(materialized="table") }}
{{ config(schema="staging") }}


SELECT
    *
FROM {{ source("raw", "raw_customers") }}