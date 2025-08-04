print("\n Registros com is_stream ou is_download NULL:")
print(con.execute("""
    SELECT store, date, is_stream, is_download, quantity, revenue
    FROM onerpm_raw
    WHERE is_stream IS NULL OR is_download IS NULL
    LIMIT 20
""").fetchdf())
