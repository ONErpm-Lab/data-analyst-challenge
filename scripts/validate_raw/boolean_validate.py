print("\n is_stream = False e is_download = False:")
print(con.execute("""
    SELECT store, date, quantity, revenue, is_stream, is_download
    FROM onerpm_raw
    WHERE is_stream = False AND is_download = False
    LIMIT 20
""").fetchdf())
