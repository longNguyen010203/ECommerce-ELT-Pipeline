import polars as pl
import pandas as pd

from dagster import (
    asset,
    multi_asset,
    Output,
    AssetIn,
    AssetOut,
    MaterializeResult,
    AssetExecutionContext,
    MetadataValue
)

from dagster_snowflake import SnowflakeResource
from snowflake.connector.pandas_tools import write_pandas


GROUP_NAME = "raw"

@multi_asset(
    ins={
        "olist_customers_dataset": AssetIn(
            key_prefix=["extract", "brazilian"]
        )
    },
    outs={
        "raw_customers": AssetOut(
            key_prefix=["load", "brazilian"],
            metadata={
                "primary_key": [
                    "customer_id"
                ],
                "foreign_key": [
                    "customer_zip_code_prefix"
                ],
                "columns": [
                    "customer_id",
                    "customer_unique_id",
                    "customer_zip_code_prefix",
                    "customer_city",
                    "customer_state"
                ]
            }
        )
    },
    name="raw_customers",
    compute_kind="snowflake",
    group_name=GROUP_NAME
)
def raw_customers(context: AssetExecutionContext,
                  olist_customers_dataset: pl.DataFrame,
                  snowflake: SnowflakeResource,
                
) -> MaterializeResult:
    """ 
        Load data from PostgreSQL db to Snowflake
    """
    
    df = pd.DataFrame(olist_customers_dataset.to_pandas())
    context.log.info(f"INPUT: {df.shape}")
    
    with snowflake.get_connection() as conn:
        table_name = "raw_customers"
        database = "Brazilian_Ecommerce"
        schema = "raw"
        success, number_chunks, rows_inserted, output = write_pandas(
            conn,
            df=df,
            table_name=table_name,
            database=database,
            schema=schema,
            auto_create_table=True,
            overwrite=True,
            quote_identifiers=False,
        )
        context.log.info("Load data to Snowflake Success!!!")
        
    return MaterializeResult(
        metadata={
            "success": MetadataValue.bool(success),
            "number_chunks": MetadataValue.int(number_chunks),
            "rows_inserted": MetadataValue.int(rows_inserted),
            "column_count": MetadataValue.int(df.shape[1]),
            "table": MetadataValue.text(table_name),
            "schema": MetadataValue.text(schema),
            "database": MetadataValue.text(database)
        }
    )
    
    
@multi_asset(
    ins={
        "olist_geolocation_dataset": AssetIn(
            key_prefix=["extract", "brazilian"]
        )
    },
    outs={
        "raw_geolocation": AssetOut(
            key_prefix=["load", "brazilian"],
            metadata={
                "primary_key": [
                    "geolocation_zip_code_prefix"
                ],
                "columns": [
                    "geolocation_zip_code_prefix",
                    "geolocation_lat",
                    "geolocation_lng",
                    "geolocation_city",
                    "geolocation_state"
                ]
            }
        )
    },
    name="raw_geolocation",
    compute_kind="snowflake",
    group_name=GROUP_NAME
)
def raw_geolocation(context: AssetExecutionContext,
                  olist_geolocation_dataset: pl.DataFrame,
                  snowflake: SnowflakeResource,
                
) -> MaterializeResult:
    """ 
        Load data from PostgreSQL db to Snowflake
    """
    
    df = pd.DataFrame(olist_geolocation_dataset.to_pandas())
    context.log.info(f"INPUT: {df.shape}")
    
    with snowflake.get_connection() as conn:
        table_name = "raw_geolocation"
        database = "Brazilian_Ecommerce"
        schema = "raw"
        success, number_chunks, rows_inserted, output = write_pandas(
            conn,
            df=df,
            table_name=table_name,
            database=database,
            schema=schema,
            auto_create_table=True,
            overwrite=True,
            quote_identifiers=False,
        )
        context.log.info("Load data to Snowflake Success!!!")
        
    return MaterializeResult(
        metadata={
            "success": MetadataValue.bool(success),
            "number_chunks": MetadataValue.int(number_chunks),
            "rows_inserted": MetadataValue.int(rows_inserted),
            "column_count": MetadataValue.int(df.shape[1]),
            "table": MetadataValue.text(table_name),
            "schema": MetadataValue.text(schema),
            "database": MetadataValue.text(database)
        }
    )
    

@multi_asset(
    ins={
        "olist_order_items_dataset": AssetIn(
            key_prefix=["extract", "brazilian"]
        )
    },
    outs={
        "raw_order_items": AssetOut(
            key_prefix=["load", "brazilian"],
            metadata={
                "primary_key": [
                    "order_item_id"
                ],
                "foreign_key": [
                    "order_id",
                    "product_id",
                    "seller_id"
                ],
                "columns": [
                    "order_id",
                    "order_item_id",
                    "product_id",
                    "seller_id",
                    "shipping_limit_date",
                    "price",
                    "freight_value"
                ]
            }
        )
    },
    name="raw_order_items",
    compute_kind="snowflake",
    group_name=GROUP_NAME
)
def raw_order_items(context: AssetExecutionContext,
                  olist_order_items_dataset: pl.DataFrame,
                  snowflake: SnowflakeResource,
                
) -> MaterializeResult:
    """ 
        Load data from PostgreSQL db to Snowflake
    """
    
    df = pd.DataFrame(olist_order_items_dataset.to_pandas())
    context.log.info(f"INPUT: {df.shape}")
    
    with snowflake.get_connection() as conn:
        table_name = "raw_order_items"
        database = "Brazilian_Ecommerce"
        schema = "raw"
        success, number_chunks, rows_inserted, output = write_pandas(
            conn,
            df=df,
            table_name=table_name,
            database=database,
            schema=schema,
            auto_create_table=True,
            overwrite=True,
            quote_identifiers=False,
        )
        context.log.info("Load data to Snowflake Success!!!")
        
    return MaterializeResult(
        metadata={
            "success": MetadataValue.bool(success),
            "number_chunks": MetadataValue.int(number_chunks),
            "rows_inserted": MetadataValue.int(rows_inserted),
            "column_count": MetadataValue.int(df.shape[1]),
            "table": MetadataValue.text(table_name),
            "schema": MetadataValue.text(schema),
            "database": MetadataValue.text(database)
        }
    )
    
    
@multi_asset(
    ins={
        "olist_order_payments_dataset": AssetIn(
            key_prefix=["extract", "brazilian"]
        )
    },
    outs={
        "raw_order_payments": AssetOut(
            key_prefix=["load", "brazilian"],
            metadata={
                "primary_key": [
                    "order_id"
                ],
                "columns": [
                    "order_id",
                    "payment_sequential",
                    "payment_type",
                    "payment_installments",
                    "payment_value"
                ]
            }
        )
    },
    name="raw_order_payments",
    compute_kind="snowflake",
    group_name=GROUP_NAME
)
def raw_order_payments(context: AssetExecutionContext,
                    olist_order_payments_dataset: pl.DataFrame,
                    snowflake: SnowflakeResource,
                
) -> MaterializeResult:
    """ 
        Load data from PostgreSQL db to Snowflake
    """
    
    df = pd.DataFrame(olist_order_payments_dataset.to_pandas())
    context.log.info(f"INPUT: {df.shape}")
    
    with snowflake.get_connection() as conn:
        table_name = "raw_order_payments"
        database = "Brazilian_Ecommerce"
        schema = "raw"
        success, number_chunks, rows_inserted, output = write_pandas(
            conn,
            df=df,
            table_name=table_name,
            database=database,
            schema=schema,
            auto_create_table=True,
            overwrite=True,
            quote_identifiers=False,
        )
        context.log.info("Load data to Snowflake Success!!!")
        
    return MaterializeResult(
        metadata={
            "success": MetadataValue.bool(success),
            "number_chunks": MetadataValue.int(number_chunks),
            "rows_inserted": MetadataValue.int(rows_inserted),
            "column_count": MetadataValue.int(df.shape[1]),
            "table": MetadataValue.text(table_name),
            "schema": MetadataValue.text(schema),
            "database": MetadataValue.text(database)
        }
    )
    
    
@multi_asset(
    ins={
        "olist_orders_dataset": AssetIn(
            key_prefix=["extract", "brazilian"]
        )
    },
    outs={
        "raw_orders": AssetOut(
            key_prefix=["load", "brazilian"],
            metadata={
                "primary_key": [
                    "order_id"
                ],
                "foreign_key": [
                    "customer_id"
                ],
                "columns": [
                    "order_id",
                    "customer_id",
                    "order_status",
                    "order_purchase_timestamp",
                    "order_approved_at",
                    "order_delivered_carrier_date",
                    "order_delivered_customer_date",
                    "order_estimated_delivery_date"
                ]
            }
        )
    },
    name="raw_orders",
    compute_kind="snowflake",
    group_name=GROUP_NAME
)
def raw_orders(context: AssetExecutionContext,
                  olist_orders_dataset: pl.DataFrame,
                  snowflake: SnowflakeResource,
                
) -> MaterializeResult:
    """ 
        Load data from PostgreSQL db to Snowflake
    """
    
    df = pd.DataFrame(olist_orders_dataset.to_pandas())
    context.log.info(f"INPUT: {df.shape}")
    
    with snowflake.get_connection() as conn:
        table_name = "raw_orders"
        database = "Brazilian_Ecommerce"
        schema = "raw"
        success, number_chunks, rows_inserted, output = write_pandas(
            conn,
            df=df,
            table_name=table_name,
            database=database,
            schema=schema,
            auto_create_table=True,
            overwrite=True,
            quote_identifiers=False,
        )
        context.log.info("Load data to Snowflake Success!!!")
        
    return MaterializeResult(
        metadata={
            "success": MetadataValue.bool(success),
            "number_chunks": MetadataValue.int(number_chunks),
            "rows_inserted": MetadataValue.int(rows_inserted),
            "column_count": MetadataValue.int(df.shape[1]),
            "table": MetadataValue.text(table_name),
            "schema": MetadataValue.text(schema),
            "database": MetadataValue.text(database)
        }
    )
    
    
@multi_asset(
    ins={
        "olist_products_dataset": AssetIn(
            key_prefix=["extract", "brazilian"]
        )
    },
    outs={
        "raw_products": AssetOut(
            key_prefix=["load", "brazilian"],
            metadata={
                "primary_key": [
                    "order_id"
                ],
                "foreign_key": [
                    "customer_id"
                ],
                "columns": [
                    "order_id",
                    "customer_id",
                    "order_status",
                    "order_purchase_timestamp",
                    "order_approved_at",
                    "order_delivered_carrier_date",
                    "order_delivered_customer_date",
                    "order_estimated_delivery_date"
                ]
            }
        )
    },
    name="raw_products",
    compute_kind="snowflake",
    group_name=GROUP_NAME
)
def raw_products(context: AssetExecutionContext,
                  olist_products_dataset: pl.DataFrame,
                  snowflake: SnowflakeResource,
                
) -> MaterializeResult:
    """ 
        Load data from PostgreSQL db to Snowflake
    """
    
    df = pd.DataFrame(olist_products_dataset.to_pandas())
    context.log.info(f"INPUT: {df.shape}")
    
    with snowflake.get_connection() as conn:
        table_name = "raw_products"
        database = "Brazilian_Ecommerce"
        schema = "raw"
        success, number_chunks, rows_inserted, output = write_pandas(
            conn,
            df=df,
            table_name=table_name,
            database=database,
            schema=schema,
            auto_create_table=True,
            overwrite=True,
            quote_identifiers=False,
        )
        context.log.info("Load data to Snowflake Success!!!")
        
    return MaterializeResult(
        metadata={
            "success": MetadataValue.bool(success),
            "number_chunks": MetadataValue.int(number_chunks),
            "rows_inserted": MetadataValue.int(rows_inserted),
            "column_count": MetadataValue.int(df.shape[1]),
            "table": MetadataValue.text(table_name),
            "schema": MetadataValue.text(schema),
            "database": MetadataValue.text(database)
        }
    )
    
    
@multi_asset(
    ins={
        "olist_order_reviews_dataset": AssetIn(
            key_prefix=["extract", "brazilian"]
        )
    },
    outs={
        "raw_order_reviews": AssetOut(
            key_prefix=["load", "brazilian"],
            metadata={
                "primary_key": [
                    "order_id"
                ],
                "foreign_key": [
                    "customer_id"
                ],
                "columns": [
                    "order_id",
                    "customer_id",
                    "order_status",
                    "order_purchase_timestamp",
                    "order_approved_at",
                    "order_delivered_carrier_date",
                    "order_delivered_customer_date",
                    "order_estimated_delivery_date"
                ]
            }
        )
    },
    name="raw_order_reviews",
    compute_kind="snowflake",
    group_name=GROUP_NAME
)
def raw_order_reviews(context: AssetExecutionContext,
                  olist_order_reviews_dataset: pl.DataFrame,
                  snowflake: SnowflakeResource,
                
) -> MaterializeResult:
    """ 
        Load data from PostgreSQL db to Snowflake
    """
    
    df = pd.DataFrame(olist_order_reviews_dataset.to_pandas())
    context.log.info(f"INPUT: {df.shape}")
    
    with snowflake.get_connection() as conn:
        table_name = "raw_order_reviews"
        database = "Brazilian_Ecommerce"
        schema = "raw"
        success, number_chunks, rows_inserted, output = write_pandas(
            conn,
            df=df,
            table_name=table_name,
            database=database,
            schema=schema,
            auto_create_table=True,
            overwrite=True,
            quote_identifiers=False,
        )
        context.log.info("Load data to Snowflake Success!!!")
        
    return MaterializeResult(
        metadata={
            "success": MetadataValue.bool(success),
            "number_chunks": MetadataValue.int(number_chunks),
            "rows_inserted": MetadataValue.int(rows_inserted),
            "column_count": MetadataValue.int(df.shape[1]),
            "table": MetadataValue.text(table_name),
            "schema": MetadataValue.text(schema),
            "database": MetadataValue.text(database)
        }
    )
    
    
@multi_asset(
    ins={
        "olist_sellers_dataset": AssetIn(
            key_prefix=["extract", "brazilian"]
        )
    },
    outs={
        "raw_sellers": AssetOut(
            key_prefix=["load", "brazilian"],
            metadata={
                "primary_key": [
                    "order_id"
                ],
                "foreign_key": [
                    "customer_id"
                ],
                "columns": [
                    "order_id",
                    "customer_id",
                    "order_status",
                    "order_purchase_timestamp",
                    "order_approved_at",
                    "order_delivered_carrier_date",
                    "order_delivered_customer_date",
                    "order_estimated_delivery_date"
                ]
            }
        )
    },
    name="raw_sellers",
    compute_kind="snowflake",
    group_name=GROUP_NAME
)
def raw_sellers(context: AssetExecutionContext,
                  olist_sellers_dataset: pl.DataFrame,
                  snowflake: SnowflakeResource,
                
) -> MaterializeResult:
    """ 
        Load data from PostgreSQL db to Snowflake
    """
    
    df = pd.DataFrame(olist_sellers_dataset.to_pandas())
    context.log.info(f"INPUT: {df.shape}")
    
    with snowflake.get_connection() as conn:
        table_name = "raw_sellers"
        database = "Brazilian_Ecommerce"
        schema = "raw"
        success, number_chunks, rows_inserted, output = write_pandas(
            conn,
            df=df,
            table_name=table_name,
            database=database,
            schema=schema,
            auto_create_table=True,
            overwrite=True,
            quote_identifiers=False,
        )
        context.log.info("Load data to Snowflake Success!!!")
        
    return MaterializeResult(
        metadata={
            "success": MetadataValue.bool(success),
            "number_chunks": MetadataValue.int(number_chunks),
            "rows_inserted": MetadataValue.int(rows_inserted),
            "column_count": MetadataValue.int(df.shape[1]),
            "table": MetadataValue.text(table_name),
            "schema": MetadataValue.text(schema),
            "database": MetadataValue.text(database)
        }
    )
    
    
@multi_asset(
    ins={
        "product_category_name_translation": AssetIn(
            key_prefix=["extract", "brazilian"]
        )
    },
    outs={
        "raw_product_category_name_translation": AssetOut(
            key_prefix=["load", "brazilian"],
            metadata={
                "primary_key": [
                    "order_id"
                ],
                "foreign_key": [
                    "customer_id"
                ],
                "columns": [
                    "order_id",
                    "customer_id",
                    "order_status",
                    "order_purchase_timestamp",
                    "order_approved_at",
                    "order_delivered_carrier_date",
                    "order_delivered_customer_date",
                    "order_estimated_delivery_date"
                ]
            }
        )
    },
    name="raw_product_category_name_translation",
    compute_kind="snowflake",
    group_name=GROUP_NAME
)
def raw_product_category_name_translation(context: AssetExecutionContext,
                  product_category_name_translation: pl.DataFrame,
                  snowflake: SnowflakeResource,
                
) -> MaterializeResult:
    """ 
        Load data from PostgreSQL db to Snowflake
    """
    
    df = pd.DataFrame(product_category_name_translation.to_pandas())
    context.log.info(f"INPUT: {df.shape}")
    
    with snowflake.get_connection() as conn:
        table_name = "raw_product_category_name_translation"
        database = "Brazilian_Ecommerce"
        schema = "raw"
        success, number_chunks, rows_inserted, output = write_pandas(
            conn,
            df=df,
            table_name=table_name,
            database=database,
            schema=schema,
            auto_create_table=True,
            overwrite=True,
            quote_identifiers=False,
        )
        context.log.info("Load data to Snowflake Success!!!")
        
    return MaterializeResult(
        metadata={
            "success": MetadataValue.bool(success),
            "number_chunks": MetadataValue.int(number_chunks),
            "rows_inserted": MetadataValue.int(rows_inserted),
            "column_count": MetadataValue.int(df.shape[1]),
            "table": MetadataValue.text(table_name),
            "schema": MetadataValue.text(schema),
            "database": MetadataValue.text(database)
        }
    )