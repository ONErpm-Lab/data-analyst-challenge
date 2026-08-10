# Arquitetura

## Visão Geral

O projeto foi desenvolvido para processar e analisar um conjunto de
dados de receita musical em larga escala e disponibilizar indicadores
executivos no Power BI.

A arquitetura segue um pipeline analítico em camadas:

``` text
Arquivos fonte .gz
        |
        v
Ingestão Python
        |
        v
DuckDB / raw.sales
        |
        v
dbt
  staging
        |
        v
  intermediate
        |
        v
  marts
        |
        v
mart_sales.parquet
        |
        v
Power BI
```

## Estrutura do Projeto

``` text
onerpm-data-challenge-colab/
|
├── README.md
├── requirements.txt
├── .gitignore
├── config/
├── ingestion/
├── warehouse/
├── dbt/
├── powerbi/
├── docs/
└── notebooks/
```

## Camada de Origem

A fonte original era composta por arquivos compactados `.gz` acessados
por URLs temporárias.

Os atributos de origem incluíam `store`, `date`, `product`, `quantity`,
`is_stream`, `is_download`, `revenue`, `currency`, `country_code`,
`genre_id` e `genre_name`.

O volume total identificado foi de aproximadamente 2,235 bilhões de
registros.

## Camada de Ingestão Python

O pacote `ingestion/` separa as responsabilidades do pipeline.

-   `main.py`: orquestra a ingestão.
-   `downloader.py`: constrói a consulta DuckDB para leitura dos
    arquivos compactados.
-   `duckdb_loader.py`: cria a camada raw e executa a carga.
-   `schema.py`: define o mapeamento e os tipos das colunas.
-   `utils.py`: concentra funções auxiliares.
-   `config/settings.py`: centraliza parâmetros do pipeline.
-   `config/urls.py`: mantém a lista de URLs de origem.

## Camada DuckDB

O DuckDB foi escolhido como engine analítica para processar grandes
volumes e arquivos compactados sem a necessidade de um servidor de banco
de dados tradicional.

A fonte lógica utilizada pelo dbt foi:

``` text
raw.sales
```

## Camada de Transformação dbt

### Staging

Modelo:

``` text
stg_sales
```

Responsabilidades:

-   expor os campos da origem;
-   derivar `year` e `month`;
-   preservar a granularidade da fonte;
-   fornecer uma interface estável para as transformações seguintes.

### Intermediate

Modelo:

``` text
int_sales_metrics
```

Responsabilidades:

-   preservar a granularidade analítica;
-   derivar quantidades de streams e downloads;
-   centralizar regras reutilizáveis antes da agregação final.

A camada intermediate não realiza a agregação final de negócio.

### Mart

Modelo:

``` text
mart_sales
```

Granularidade:

``` text
year
month
store
country_code
genre_id
genre_name
```

A mart concentra as agregações consumidas pelo Power BI, incluindo
receita, quantidade, streams, downloads e indicadores derivados.

## Exportação Analítica

A mart final foi exportada como:

``` text
mart_sales.parquet
```

O formato Parquet foi utilizado por ser colunar, compactado e eficiente
para cargas analíticas.

## Camada Power BI

O projeto Power BI é:

``` text
Revenue Growth Intelligence.pbip
```

O relatório possui quatro páginas:

1.  `01 | Executive Overview`
2.  `02 | Growth Strategy`
3.  `03 | Regional Performance`
4.  `04 | Stream Conversion`

O modelo semântico utiliza uma tabela Calendar e medidas DAX organizadas
por pastas de exibição.

## Limitação de Reprodutibilidade

As URLs dos arquivos `.gz` eram temporárias e expiraram após o
desenvolvimento.

Por esse motivo, a ingestão completa não pode ser reproduzida sem novas
URLs válidas da fonte.

O desenvolvimento analítico e o dashboard foram concluídos utilizando o
`mart_sales.parquet` gerado durante o processamento original.

## Princípios de Projeto

-   separação entre ingestão, transformação e visualização;
-   staging próxima da estrutura da origem;
-   intermediate para regras reutilizáveis;
-   agregação concentrada na mart;
-   medidas DAX reutilizáveis;
-   comparação de períodos equivalentes;
-   tratamento de materialidade em rankings;
-   separação entre visão executiva e visão regional.
