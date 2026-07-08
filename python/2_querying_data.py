import duckdb
from pathlib import Path
import shutil

BASE_DIR = Path(__file__).parent
OUTPUT_DIR = BASE_DIR / "processed"

print("Limpando arquivos antigos...")

for db in BASE_DIR.glob("*.duckdb"):
    db.unlink()

for tmp in BASE_DIR.glob("*.duckdb.tmp"):
    if tmp.is_dir():
        shutil.rmtree(tmp)
    else:
        tmp.unlink()

for tmp in BASE_DIR.glob("duckdb_temp*"):
    if tmp.is_dir():
        shutil.rmtree(tmp)

if OUTPUT_DIR.exists():
    shutil.rmtree(OUTPUT_DIR)

OUTPUT_DIR.mkdir(exist_ok=True)

print("Limpeza concluída.")

files = sorted(BASE_DIR.glob("*.gz"))

con = duckdb.connect("onerpm.duckdb")

for file in files:
    part = file.name.replace(".csv.gz", "").replace(".gz", "")
    output_file = OUTPUT_DIR / f"agg_{part}.parquet"

    print(f"Processando {file.name}...")

    con.execute(f"""
        COPY (
            SELECT
                column00 AS store,
                column01::DATE AS stream_date,
                column04 AS is_stream,
                column05 AS is_download,
                column07 AS currency,
                column08 AS country_code,
                column09 AS genre_id,
                column10 AS genre_name,
                SUM(column03) AS quantity,
                SUM(column06) AS revenue
            FROM read_csv_auto(
                '{file.as_posix()}',
                compression='gzip'
            )
            GROUP BY
                column00,
                column01,
                column04,
                column05,
                column07,
                column08,
                column09,
                column10
        )
        TO '{output_file.as_posix()}'
        (FORMAT PARQUET, COMPRESSION ZSTD);
    """)

    print(f"{output_file.name} criado.")

parquet_path = (OUTPUT_DIR / "*.parquet").as_posix()

print("Criando market_summary.parquet...")

con.execute(f"""
    COPY (
        SELECT
            YEAR(stream_date) AS year,
            country_code,
            currency,
            is_stream,
            is_download,
            store,
            genre_name,
            SUM(quantity) AS quantity,
            SUM(revenue) AS revenue
        FROM read_parquet('{parquet_path}')
        GROUP BY
            YEAR(stream_date),
            country_code,
            currency,
            is_stream,
            is_download,
            store,
            genre_name
    )
    TO '{(BASE_DIR / "market_summary.parquet").as_posix()}'
    (FORMAT PARQUET, COMPRESSION ZSTD);
""")

print("Criando market_monthly.parquet...")

con.execute(f"""
    COPY (
        SELECT
            DATE_TRUNC('month', stream_date) AS month,
            store,
            country_code,
            currency,
            genre_id,
            genre_name,
            is_stream,
            is_download,
            SUM(quantity) AS quantity,
            SUM(revenue) AS revenue
        FROM read_parquet('{parquet_path}')
        GROUP BY
            DATE_TRUNC('month', stream_date),
            store,
            country_code,
            currency,
            genre_id,
            genre_name,
            is_stream,
            is_download
    )
    TO '{(BASE_DIR / "market_monthly.parquet").as_posix()}'
    (FORMAT PARQUET, COMPRESSION ZSTD);
""")

print("Validando arquivos finais...")

print(con.execute("""
    SELECT COUNT(*) AS rows_year
    FROM read_parquet('market_summary.parquet')
""").df())

print(con.execute("""
    SELECT COUNT(*) AS rows_month
    FROM read_parquet('market_monthly.parquet')
""").df())

con.close()

print("Processamento concluído.")