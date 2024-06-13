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