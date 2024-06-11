from setuptools import find_packages, setup

setup(
    name="Ecommerce_dagster",
    version="0.0.1",
    packages=find_packages(),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-dbt",
        "dbt-snowflake<1.9",
        "dbt-postgres<1.9",
        "dbt-postgres<1.9",
        "dbt-postgres<1.9",
        "dbt-postgres<1.9",
        "dbt-bigquery<1.9",
        "dbt-postgres<1.9",
    ],
    extras_require={
        "dev": [
            "dagster-webserver",
        ]
    },
)