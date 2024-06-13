from contextlib import contextmanager
import psycopg2.extras
from typing import Union
import polars as pl
import psycopg2

from dagster import IOManager, OutputContext, InputContext


@contextmanager
def connect_psql(config: dict):
    try:
        yield psycopg2.connect(
            host=config["host"],
            port=config["port"],
            database=config["database"],
            user=config["user"],
            password=config["password"],
        )
        
    except Exception as e: 
        raise e
    
    
class PostgreSQLIOManager(IOManager):
    
    def __init__(self, config) -> None:
        self._config = config
        
    def handle_output(self, context: OutputContext, obj: pl.DataFrame):
        pass

    def load_input(self, context: InputContext) -> pl.DataFrame:
        pass
    
    def _get_path(self, context: Union[InputContext, OutputContext]):
        folderName, fileName = context.asset_key.path
        path = "/tmp/{}/{}.csv".format(folderName, fileName)
        return path
    
    def extract_data(self, context) -> pl.DataFrame:
        
        with connect_psql(self._config) as db_con:
            path = self._get_path(context)
            return pl.read_csv(path)