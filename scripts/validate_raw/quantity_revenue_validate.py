print("\n quantity = 0 com revenue > 0:")
print(con.execute("""
    SELECT store, date, quantity, revenue, is_stream, is_download
    FROM onerpm_raw
    WHERE quantity = 0 AND revenue > 0
    LIMIT 20
""").fetchdf())

print("\n quantity > 0 com revenue = 0:")
print(con.execute("""
    SELECT store, date, quantity, revenue, is_stream, is_download
    FROM onerpm_raw
    WHERE quantity > 0 AND revenue = 0
    LIMIT 20
""").fetchdf())
