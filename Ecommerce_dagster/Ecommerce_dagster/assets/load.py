import polars as pl
import pandas as pd

from dagster import (
    asset,
    Output,
    AssetIn,
    MaterializeResult,
    AssetExecutionContext,
    MetadataValue
)

from dagster_snowflake import SnowflakeResource
from snowflake.connector.pandas_tools import write_pandas


GROUP_NAME = "raw"

@asset(
    ins={
        "olist_customers_dataset": AssetIn(
            key_prefix=["brazilian_extract"],
        )
    },
    name="olist_customers_dataset",
    key_prefix=["brazilian_load"],
    compute_kind="snowflake",
    group_name=GROUP_NAME
)
def raw_customers_dataset(context: AssetExecutionContext,
                          olist_customers_dataset: pl.DataFrame,
                          snowflake: SnowflakeResource,
                          
) -> None:
    """ 
        Load data from PostgreSQL db to Snowflake
    """
    with snowflake.get_connection() as conn:
        table_name = "raw_customers"
        database = "Brazilian_Ecommerce"
        schema = "raw"
        success, number_chunks, rows_inserted, output = write_pandas(
            conn,
            pd.DataFrame(olist_customers_dataset),
            table_name=table_name,
            database=database,
            schema=schema,
            auto_create_table=True,
            overwrite=True,
            quote_identifiers=False,
        )
        
    return MaterializeResult(
        metadata={
            "success": MetadataValue.bool(success),
            "number_chunks": MetadataValue.int(number_chunks),
            "rows_inserted": MetadataValue.int(rows_inserted),
            "output": output
        }
    )