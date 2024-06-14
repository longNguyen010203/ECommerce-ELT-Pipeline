import polars as pl

from dagster import (
    asset,
    Output,
    MetadataValue,
    AssetExecutionContext
)


GROUP_NAME = "source"

@asset(
    name="olist_customers_dataset",
    required_resource_keys={"psql_io_manager"},
    key_prefix=["extract", "brazilian"],
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
    key_prefix=["extract", "brazilian"],
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
    key_prefix=["extract", "brazilian"],
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
    key_prefix=["extract", "brazilian"],
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
    key_prefix=["extract", "brazilian"],
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


@asset(
    name="olist_products_dataset",
    required_resource_keys={"csv_io_manager"},
    key_prefix=["extract", "brazilian"],
    compute_kind="CSV",
    group_name=GROUP_NAME
)
def olist_products_dataset(context: AssetExecutionContext) -> Output[pl.DataFrame]:
    """
        Load data from table 'olist_products_dataset'
        in CSV file as polars DataFrame
    """
    
    pl_data: pl.DataFrame = context.resources.csv_io_manager.extract_data(context)
    context.log.info(f"Extract table 'olist_products_dataset' from CSV file Success")
    
    return Output(
        value=pl_data,
        metadata={
            "columns count": MetadataValue.int(pl_data.shape[1]),
            "records count": MetadataValue.int(pl_data.shape[0])
        }
    )
    
    
@asset(
    name="olist_order_reviews_dataset",
    required_resource_keys={"csv_io_manager"},
    key_prefix=["extract", "brazilian"],
    compute_kind="CSV",
    group_name=GROUP_NAME
)
def olist_order_reviews_dataset(context: AssetExecutionContext) -> Output[pl.DataFrame]:
    """
        Load data from table 'olist_order_reviews_dataset'
        in CSV file as polars DataFrame
    """
    
    pl_data: pl.DataFrame = context.resources.csv_io_manager.extract_data(context)
    context.log.info(f"Extract table 'olist_order_reviews_dataset' from CSV file Success")
    
    return Output(
        value=pl_data,
        metadata={
            "columns count": MetadataValue.int(pl_data.shape[1]),
            "records count": MetadataValue.int(pl_data.shape[0])
        }
    )
    
    
@asset(
    name="olist_sellers_dataset",
    required_resource_keys={"csv_io_manager"},
    key_prefix=["extract", "brazilian"],
    compute_kind="CSV",
    group_name=GROUP_NAME
)
def olist_sellers_dataset(context: AssetExecutionContext) -> Output[pl.DataFrame]:
    """
        Load data from table 'olist_sellers_dataset'
        in CSV file as polars DataFrame
    """
    
    pl_data: pl.DataFrame = context.resources.csv_io_manager.extract_data(context)
    context.log.info(f"Extract table 'olist_sellers_dataset' from CSV file Success")
    
    return Output(
        value=pl_data,
        metadata={
            "columns count": MetadataValue.int(pl_data.shape[1]),
            "records count": MetadataValue.int(pl_data.shape[0])
        }
    )
    
    
@asset(
    name="product_category_name_translation",
    required_resource_keys={"csv_io_manager"},
    key_prefix=["extract", "brazilian"],
    compute_kind="CSV",
    group_name=GROUP_NAME
)
def product_category_name_translation(context: AssetExecutionContext) -> Output[pl.DataFrame]:
    """
        Load data from table 'product_category_name_translation'
        in CSV file as polars DataFrame
    """
    
    pl_data: pl.DataFrame = context.resources.csv_io_manager.extract_data(context)
    context.log.info(f"Extract table 'product_category_name_translation' from CSV file Success")
    
    return Output(
        value=pl_data,
        metadata={
            "columns count": MetadataValue.int(pl_data.shape[1]),
            "records count": MetadataValue.int(pl_data.shape[0])
        }
    )