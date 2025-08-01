import duckdb

# Conecta ao banco local DuckDB
con = duckdb.connect("onerpm.duckdb")

# Total de linhas
total_linhas = con.execute("SELECT COUNT(*) FROM onerpm_raw").fetchone()[0]
print("\nVALIDAÇÃO DA TABELA 'onerpm_raw'")
print("Total de linhas:", f"{total_linhas:,}")

# Linhas por ano
print("\nDistribuição por ano:")
print(con.execute("""
    SELECT strftime('%Y', date) AS ano, COUNT(*) AS linhas
    FROM onerpm_raw
    GROUP BY 1
    ORDER BY 1
""").fetchdf().to_string(index=False))