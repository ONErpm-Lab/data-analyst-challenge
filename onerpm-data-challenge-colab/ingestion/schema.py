class SalesSchema:

    @staticmethod
    def select_columns():
        return """
            column00 AS store,
            CAST(column01 AS DATE) AS date,
            CAST(column02 AS BIGINT) AS product,
            CAST(column03 AS BIGINT) AS quantity,
            CAST(column04 AS BOOLEAN) AS is_stream,
            CAST(column05 AS BOOLEAN) AS is_download,
            CAST(column06 AS DOUBLE) AS revenue,
            column07 AS currency,
            column08 AS country_code,
            CAST(column09 AS BIGINT) AS genre_id,
            column10 AS genre_name
        """