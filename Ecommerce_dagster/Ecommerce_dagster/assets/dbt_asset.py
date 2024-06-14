from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets
from dagster_dbt import DagsterDbtTranslator

from typing import Mapping, Optional, Any

from ..constants import dbt_manifest_path


# class CustomDagsterDbtTranslator(DagsterDbtTranslator):
#     def get_group_name(
#         self, dbt_resource_props: Mapping[str, Any]
#     ) -> Optional[str]:
#         return "staging"


@dbt_assets(
    manifest=dbt_manifest_path,
    # dagster_dbt_translator=CustomDagsterDbtTranslator()
)
def Brazilian_ECommerce_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
    