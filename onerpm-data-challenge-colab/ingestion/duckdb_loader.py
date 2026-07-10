import duckdb

from config.settings import DATABASE
from config.settings import RAW_SCHEMA
from config.settings import THREADS

from ingestion.downloader import Downloader


class DuckDBLoader:

    def __init__(self):

        self.conn = duckdb.connect(DATABASE)

        self.conn.execute(f"SET threads = {THREADS}")

    def create_schema(self):

        self.conn.execute(
            f"""
            CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}
            """
        )

    def create_raw_sales_view(self):

        sql = Downloader.get_query()

        self.conn.execute(
            f"""
            CREATE OR REPLACE VIEW {RAW_SCHEMA}.sales AS

            {sql}
            """
        )

    def close(self):

        if self.conn:

            self.conn.close()