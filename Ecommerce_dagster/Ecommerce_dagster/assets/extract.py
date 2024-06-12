import polars as pl

from dagster import (
    asset,
    Output,
    MetadataValue,
    AssetExecutionContext
)


GROUP_NAME = "raw"

@asset(
    name="olist_customers_dataset",
    required_resource_keys={"psql_io_manager"},
    io_manager_key="psql_io_manager",
    key_prefix=["brazilian_extract"],
    compute_kind="postgres",
    group_name=GROUP_NAME
)
def olist_customers_dataset(context: AssetExecutionContext) -> Output[pl.DataFrame]:
    """
        Load data from table 'olist_customers_dataset'
        in PostgreSQL database as polars DataFrame
    """
    query = """ SELECT * FROM public.olist_customers_dataset; """
    
    pl_data: pl.DataFrame = context.resources.psql_io_manager.extract_data(query)
    context.log.info(f"Extract table 'olist_customers_dataset' from PostgreSQL Success")
    
    return Output(
        value=pl_data,
        metadata={
            "columns count": MetadataValue.int(pl_data.shape[1]),
            "records count": MetadataValue.int(pl_data.shape[0])
        }
    )
    
    
@asset(
    name="olist_geolocation_dataset",
    required_resource_keys={"psql_io_manager"},
    io_manager_key="psql_io_manager",
    key_prefix=["brazilian_extract"],
    compute_kind="postgres",
    group_name=GROUP_NAME
)
def olist_geolocation_dataset(context: AssetExecutionContext) -> Output[pl.DataFrame]:
    """
        Load data from table 'olist_geolocation_dataset'
        in PostgreSQL database as polars DataFrame
    """
    query = """ SELECT * FROM public.olist_geolocation_dataset; """
    
    pl_data: pl.DataFrame = context.resources.psql_io_manager.extract_data(query)
    context.log.info(f"Extract table 'olist_geolocation_dataset' from PostgreSQL Success")
    
    return Output(
        value=pl_data,
        metadata={
            "columns count": MetadataValue.int(pl_data.shape[1]),
            "records count": MetadataValue.int(pl_data.shape[0])
        }
    )
    
    
@asset(
    name="olist_order_items_dataset",
    required_resource_keys={"psql_io_manager"},
    io_manager_key="psql_io_manager",
    key_prefix=["brazilian_extract"],
    compute_kind="postgres",
    group_name=GROUP_NAME
)
def olist_order_items_dataset(context: AssetExecutionContext) -> Output[pl.DataFrame]:
    """
        Load data from table 'olist_order_items_dataset'
        in PostgreSQL database as polars DataFrame
    """
    query = """ SELECT * FROM public.olist_order_items_dataset; """
    
    pl_data: pl.DataFrame = context.resources.psql_io_manager.extract_data(query)
    context.log.info(f"Extract table 'olist_order_items_dataset' from PostgreSQL Success")
    
    return Output(
        value=pl_data,
        metadata={
            "columns count": MetadataValue.int(pl_data.shape[1]),
            "records count": MetadataValue.int(pl_data.shape[0])
        }
    )
    
    
@asset(
    name="olist_order_payments_dataset",
    required_resource_keys={"psql_io_manager"},
    io_manager_key="psql_io_manager",
    key_prefix=["brazilian_extract"],
    compute_kind="postgres",
    group_name=GROUP_NAME
)
def olist_order_payments_dataset(context: AssetExecutionContext) -> Output[pl.DataFrame]:
    """
        Load data from table 'olist_order_payments_dataset'
        in PostgreSQL database as polars DataFrame
    """
    query = """ SELECT * FROM public.olist_order_payments_dataset; """
    
    pl_data: pl.DataFrame = context.resources.psql_io_manager.extract_data(query)
    context.log.info(f"Extract table 'olist_order_payments_dataset' from PostgreSQL Success")
    
    return Output(
        value=pl_data,
        metadata={
            "columns count": MetadataValue.int(pl_data.shape[1]),
            "records count": MetadataValue.int(pl_data.shape[0])
        }
    )
    
    
@asset(
    name="olist_orders_dataset",
    required_resource_keys={"psql_io_manager"},
    io_manager_key="psql_io_manager",
    key_prefix=["brazilian_extract"],
    compute_kind="postgres",
    group_name=GROUP_NAME
)
def olist_orders_dataset(context: AssetExecutionContext) -> Output[pl.DataFrame]:
    """
        Load data from table 'olist_orders_dataset'
        in PostgreSQL database as polars DataFrame
    """
    query = """ SELECT * FROM public.olist_orders_dataset; """
    
    pl_data: pl.DataFrame = context.resources.psql_io_manager.extract_data(query)
    context.log.info(f"Extract table 'olist_orders_dataset' from PostgreSQL Success")
    
    return Output(
        value=pl_data,
        metadata={
            "columns count": MetadataValue.int(pl_data.shape[1]),
            "records count": MetadataValue.int(pl_data.shape[0])
        }
    )
