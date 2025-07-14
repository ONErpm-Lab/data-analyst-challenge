import pandas as pd
import gzip
import requests
from io import BytesIO, TextIOWrapper
from google.cloud import bigquery

def process_gz_stream_to_bigquery(link: str, table_id: str, credentials: str, chunksize: int = 100_000):
    client = bigquery.Client.from_service_account_json(credentials)
    nome_arquivo = link.split("/")[-1].split("?")[0]

    print(f"🔽 Streaming: {nome_arquivo}")
    response = requests.get(link, stream=True)
    response.raise_for_status()

    # Prepara leitura em streaming por partes
    gz_stream = BytesIO(response.content)
    text_stream = TextIOWrapper(gzip.GzipFile(fileobj=gz_stream), encoding="utf-8")
    columns = [
    "store", "date", "product", "quantity",
    "is_stream", "is_download", "revenue",
    "currency", "country_code", "genre_id", "genre_name"]


    # Processa em pedaços e envia direto para BigQuery
    for idx, df_chunk in enumerate(pd.read_csv(text_stream,names=columns ,chunksize=chunksize), start=1):
        df_chunk["origem_arquivo"] = nome_arquivo

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            autodetect=True
        )
        job = client.load_table_from_dataframe(df_chunk, table_id, job_config=job_config)
        job.result()

        print(f"📦 Chunk {idx} enviado ({len(df_chunk)} linhas)")

    print(f"✅ Upload completo: {nome_arquivo}")

# 🧠 Executa para todos os links
def process_lote_links(links_file: str, table_id: str, credentials: str):
    with open(links_file, "r") as f:
        links = [l.strip() for l in f.readlines() if l.strip()]

    for link in links:
        process_gz_stream_to_bigquery(link, table_id, credentials)

# 🔧 Exemplo de uso
if __name__ == "__main__":
    links_file = "scripts/links.txt"
    table_id = "onerpm-dbt.onerm_dataset.tb_dados_one_rpm"
    credentials = "/content/onerpm-dbt-14a34acb17b7.json"

    process_lote_links(links_file, table_id, credentials)