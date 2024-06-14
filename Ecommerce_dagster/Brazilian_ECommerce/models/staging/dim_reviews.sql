{{ config(materialized="table") }}
{{ config(schema="staging") }}


SELECT 
    review_id
    , review_score
    , review_comment_title
    , review_comment_message
    , review_creation_date
    , review_answer_timestamp
    , order_id

FROM {{ source("raw", "raw_order_reviews") }}