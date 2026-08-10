from config.urls import URLS
from ingestion.schema import SalesSchema


class Downloader:

    @staticmethod
    def get_query():

        urls = ",".join(f"'{url}'" for url in URLS)

        return f"""
            SELECT

                {SalesSchema.select_columns()}

            FROM read_csv_auto(

                [{urls}],

                compression='gzip'

            )
        """