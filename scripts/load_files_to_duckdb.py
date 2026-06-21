"""
This script loads the raw zipped files into a DuckDB database.
Each file is loaded into it's own table.
Duckdb can read gzipped files directly and efficiently:
    https://duckdb.org/docs/current/guides/performance/file_formats
    Session: Loading CSV Files
"""

import duckdb
import os
from pathlib import Path


def load_raw_files(
    db_path: str = "data/database/onerpm_analytics.duckdb",
) -> duckdb.DuckDBPyConnection:
    """
    Load the raw zipped files into a DuckDB database.
    Each file is loaded into it's own table.

    Args:
        db_path (str, optional):
        Analytics database for the data team.
        Defaults to "data/database/onerpm_analytics.duckdb".

    Returns:
        duckdb.DuckDBPyConnection:
        Connection to the database.
    """

    raw_dir: Path = Path("data/raw")
    raw_dir: str = "data/raw"
    con: duckdb.DuckDBPyConnection = duckdb.connect(db_path)

    for filename in sorted(os.listdir(raw_dir)):
        if not filename.endswith(".gz"):
            print(f"Skipping {filename}: not a gzipped file")
            continue

        file_path: Path = os.path.join(raw_dir, filename).replace(os.sep, "/")
        table_name: str = (
            filename.replace("-", "_").replace(".csv.gz", "").replace(".gz", "")
        )
        print(f"Loading {filename} to table '{table_name}'...")

        con.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
                SELECT * FROM read_csv_auto('{file_path}', compression='gzip')
            """)

        row_count: int = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"Loaded {filename} to table '{table_name}' ({row_count:,} rows)")

    return con


def main():
    """
    Main function to be run to load the raw data into the databse.
    """
    con: duckdb.DuckDBPyConnection = load_raw_files()
    tables: list = con.execute("SHOW TABLES").fetchall()

    print(f"Done! {len(tables)} table(s) available: {[t[0] for t in tables]}")

    con.close()


if __name__ == "__main__":
    main()
