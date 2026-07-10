from ingestion.duckdb_loader import DuckDBLoader
from ingestion.utils import Timer


def main():

    loader = DuckDBLoader()

    try:

        with Timer():

            print("Conectado ao DuckDB.")

            print("Criando schema RAW...")
            loader.create_schema()

            print("Criando VIEW RAW.SALES...")
            loader.create_raw_sales_view()

            print("Processo finalizado com sucesso.")

    finally:

        loader.close()

        print("Conexão encerrada.")


if __name__ == "__main__":

    main()