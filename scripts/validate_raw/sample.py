import duckdb
con = duckdb.connect("onerpm.duckdb")
df = con.execute("SELECT * FROM onerpm_raw LIMIT 30").fetchdf()
print(df)