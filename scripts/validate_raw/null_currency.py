print("\n Registros com currency NULL:")
print(con.execute("""
    SELECT store, date, revenue, country_code, currency
    FROM onerpm_raw
    WHERE currency IS NULL
    LIMIT 20
""").fetchdf())
