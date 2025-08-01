import duckdb
import glob
import os

db_path = "onerpm.duckdb"
conn = duckdb.connect(database=db_path)

files = sorted(glob.glob("data/raw/*.gz"))

schema_ddl = """
    store TEXT,
    date DATE,
    product TEXT,
    quantity INTEGER,
    is_stream BOOLEAN,
    is_download BOOLEAN,
    revenue DOUBLE,
    currency TEXT,
    country_code TEXT,
    genre_id INTEGER,
    genre_name TEXT
"""

columns_dict = {
    "store": "TEXT",
    "date": "DATE",
    "product": "TEXT",
    "quantity": "INTEGER",
    "is_stream": "BOOLEAN",
    "is_download": "BOOLEAN",
    "revenue": "DOUBLE",
    "currency": "TEXT",
    "country_code": "TEXT",
    "genre_id": "INTEGER",
    "genre_name": "TEXT"
}

print("🔧 Criando tabela vazia onerpm_raw...")
conn.execute(f"CREATE OR REPLACE TABLE onerpm_raw ({schema_ddl})")
print("Tabela criada.")

for file in files:
    print(f" Processando: {os.path.basename(file)}")
    conn.execute(f"""
        INSERT INTO onerpm_raw
        SELECT * 
        FROM read_csv('{file}', columns=$schema, header=False,
            ignore_errors=true, null_padding=true);
    """, {"schema": columns_dict})
    print(f"✅ {os.path.basename(file)} inserido.")

print(" Ingestão finalizada com sucesso.")
