import duckdb

# Conectar ao banco
con = duckdb.connect("onerpm.duckdb")

# Verificar se currency contém apenas USD
print("\n Currencies distintas:")
print(con.execute("SELECT DISTINCT currency FROM onerpm_raw").fetchdf())

# Verificar stores únicas
print("\n Stores distintas:")
print(con.execute("SELECT DISTINCT store FROM onerpm_raw ORDER BY store").fetchdf())

# Verificar códigos de país
print("\n Country codes distintos:")
print(con.execute("SELECT DISTINCT country_code FROM onerpm_raw ORDER BY country_code").fetchdf())

# Verificar nomes de gênero
print("\n Gêneros musicais:")
print(con.execute("SELECT DISTINCT genre_name FROM onerpm_raw ORDER BY genre_name").fetchdf())

# Contagem de gêneros únicos
print("\n Quantidade de genres distintos (genre_id):")
print(con.execute("SELECT COUNT(DISTINCT genre_id) AS total_genres FROM onerpm_raw").fetchdf())

# Valores únicos de is_stream e is_download
print("\n Valores distintos de is_stream:")
print(con.execute("SELECT DISTINCT is_stream FROM onerpm_raw").fetchdf())

print("\n Valores distintos de is_download:")
print(con.execute("SELECT DISTINCT is_download FROM onerpm_raw").fetchdf())

# Intervalo de datas
print("\n Intervalo de datas:")
print(con.execute("SELECT MIN(date) AS data_minima, MAX(date) AS data_maxima FROM onerpm_raw").fetchdf())

# Maiores quantidades
print("\n Maiores valores distintos de quantity:")
print(con.execute("SELECT DISTINCT quantity FROM onerpm_raw ORDER BY quantity DESC LIMIT 20").fetchdf())
