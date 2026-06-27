"""
This script exports all dbt mart tables from DuckDB to Parquet files.
Each mart is exported to its own file under data/exports/.
Parquet is used as the interchange format for Power BI consumption.
"""

import duckdb
from pathlib import Path

MARTS = [
    "mart__revenue_overview",
    "mart__revenue_by_store",
    "mart__revenue_by_region",
    "mart__revenue_by_genre",
    "mart__revenue_year_over_year",
    "mart__revenue_region_goals",
]


def export_marts_to_parquet(
    db_path: str = "data/database/onerpm_analytics.duckdb",
    exports_dir: str = "data/exports",
    schema: str = "main_marts",
) -> None:
    """
    Export all mart tables from DuckDB to Parquet files.

    Args:
        db_path (str, optional):
            Path to the DuckDB database file.
            Defaults to "data/database/onerpm_analytics.duckdb".
        exports_dir (str, optional):
            Directory where Parquet files will be written.
            Defaults to "data/exports".
        schema (str, optional):
            DuckDB schema where the mart tables live.
            Defaults to "marts".
    """
    exports_path: Path = Path(exports_dir)
    exports_path.mkdir(parents=True, exist_ok=True)

    con: duckdb.DuckDBPyConnection = duckdb.connect(db_path)

    for mart in MARTS:
        output_file: Path = exports_path / f"{mart}.parquet"

        print(f"Exporting {schema}.{mart} -> {output_file}...")

        con.execute(f"""
            COPY (SELECT * FROM {schema}.{mart})
            TO '{output_file.as_posix()}'
            (FORMAT PARQUET)
        """)

        row_count: int = con.execute(
            f"SELECT COUNT(*) FROM {schema}.{mart}"
        ).fetchone()[0]

        print(f"  Done — {row_count:,} rows written to {output_file.name}")

    con.close()


def main():
    """
    Main function to export all mart tables to Parquet.
    """
    export_marts_to_parquet()
    print(f"\nDone! {len(MARTS)} file(s) exported to data/exports/")


if __name__ == "__main__":
    main()
