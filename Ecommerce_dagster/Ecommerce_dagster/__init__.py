import os

from dagster import Definitions, load_assets_from_modules
from dagster_dbt import DbtCliResource

from .assets import dbt_asset, extract, load
from .resources import postgres, snowflake
from .constants import dbt_project_dir
from .schedules import schedules

all_assets = load_assets_from_modules(
    [dbt_asset, extract, load])


defs = Definitions(
    assets=all_assets,
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
        "psql_io_manager": postgres,
        "snowflake": snowflake,
    },
)