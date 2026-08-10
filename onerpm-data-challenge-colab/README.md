# ONErpm Data Challenge --- Revenue Growth Intelligence

## Visão Geral

Este projeto foi desenvolvido para analisar as principais fontes de
receita da empresa, identificar padrões globais e regionais e construir
uma visão executiva orientada a crescimento, monetização e priorização
de oportunidades.

A solução processa um conjunto de dados de grande volume, aplica
transformações analíticas com dbt e disponibiliza os principais
indicadores em um dashboard Power BI.

O relatório final busca responder às seguintes perguntas de negócio:

-   Qual a projeção de crescimento esperado para 2025?
-   Qual seria a melhor estratégia para potencializar o aumento de
    receita?
-   É possível identificar regiões com tendência e potencial de
    crescimento ou queda?
-   Qual gênero apresenta a melhor relação entre streams e receita?

## Arquitetura

O fluxo analítico foi estruturado da seguinte forma:

``` text
Arquivos fonte .gz
        |
        v
Python Ingestion
        |
        v
DuckDB
raw.sales
        |
        v
dbt
staging
        |
        v
intermediate
        |
        v
mart
        |
        v
mart_sales.parquet
        |
        v
Power BI
Revenue Growth Intelligence
```

A separação em camadas permite isolar ingestão, transformação, modelagem
analítica e visualização.

Mais detalhes estão disponíveis em `docs/architecture.md`.

## Estrutura do Projeto

``` text
onerpm-data-challenge-colab/
│
├── README.md
├── requirements.txt
├── .gitignore
│
├── config/
│   ├── settings.py
│   └── urls.py
│
├── ingestion/
│   ├── __init__.py
│   ├── main.py
│   ├── downloader.py
│   ├── duckdb_loader.py
│   ├── schema.py
│   └── utils.py
│
├── warehouse/
│   └── onerpm.duckdb
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   │
│   └── models/
│       ├── sources.yml
│       │
│       ├── staging/
│       │   └── stg_sales.sql
│       │
│       ├── intermediate/
│       │   └── int_sales_metrics.sql
│       │
│       └── marts/
│           └── mart_sales.sql
│
├── powerbi/
│   └── Revenue Growth Intelligence.pbip
│
├── docs/
│   ├── architecture.md
│   ├── data_dictionary.md
│   ├── methodology.md
│   └── dashboard_guide.md
│
└── notebooks/
    └── setup_colab.ipynb
```

## Tecnologias Utilizadas

-   Python
-   DuckDB
-   dbt Core
-   dbt-duckdb
-   Parquet
-   Power BI
-   DAX
-   Google Colab

## Volume de Dados

O conjunto de arquivos de origem possuía aproximadamente:

``` text
2.235.121.650 registros
```

Devido ao volume, a solução foi desenhada para evitar processamento
convencional integral em memória com pandas.

O DuckDB foi utilizado como engine analítica para leitura e
processamento dos arquivos compactados.

## Pipeline de Ingestão

A camada `ingestion/` é responsável pela leitura dos arquivos `.gz` e
disponibilização da fonte analítica no DuckDB.

Os principais componentes são:

### `main.py`

Orquestra a execução do pipeline.

### `downloader.py`

Constrói a consulta de leitura dos arquivos compactados.

### `duckdb_loader.py`

Gerencia a conexão com o DuckDB e a criação da camada `raw`.

### `schema.py`

Define o mapeamento das colunas e conversões de tipos.

### `utils.py`

Contém funções auxiliares do processo.

As configurações do pipeline estão centralizadas em:

``` text
config/settings.py
config/urls.py
```

## Modelagem dbt

O projeto utiliza três camadas lógicas.

### Staging

Modelo:

``` text
stg_sales
```

Responsável por expor os dados da origem e derivar atributos temporais
como `year` e `month`.

### Intermediate

Modelo:

``` text
int_sales_metrics
```

Responsável pela criação de campos analíticos reutilizáveis, como
quantidades associadas a streams e downloads.

A camada intermediate preserva a granularidade analítica e evita a
agregação final de negócio.

### Mart

Modelo:

``` text
mart_sales
```

Granularidade:

``` text
Ano x Mês x Loja x País x Gênero
```

A mart concentra as métricas consumidas pelo Power BI:

-   vendas;
-   quantidade;
-   receita;
-   streams;
-   downloads;
-   ticket médio;
-   receita por stream equivalente;
-   receita por download equivalente;
-   receita média por quantidade.

O resultado analítico foi exportado para:

``` text
mart_sales.parquet
```

## Dashboard Power BI

Projeto:

``` text
Revenue Growth Intelligence.pbip
```

O dashboard foi desenvolvido para dois públicos principais:

-   Diretores Executivos;
-   Chefes Regionais.

O relatório possui quatro páginas.

### 01 \| Executive Overview

Visão global dos principais indicadores.

Principais análises:

-   receita total;
-   streams;
-   crescimento YoY;
-   projeção de receita para 2025;
-   crescimento projetado para 2025;
-   tendência mensal de receita;
-   principais lojas;
-   receita por país;
-   principais gêneros por receita.

### 02 \| Growth Strategy

Visão orientada à identificação de oportunidades de crescimento.

O `Opportunity Score` combina:

``` text
40% Crescimento YoY
30% Monetização por Stream
30% Participação na Receita
```

Os indicadores são normalizados antes da aplicação dos pesos.

A página também classifica os gêneros em quadrantes estratégicos:

-   Stars;
-   Growth Opportunity;
-   Monetization Opportunity;
-   Low Priority.

A classificação é utilizada para gerar uma recomendação estratégica.

### 03 \| Regional Performance

Visão destinada aos Chefes Regionais.

O crescimento é calculado utilizando períodos comparáveis:

``` text
Jan-Nov/2024
vs.
Jan-Nov/2023
```

A página apresenta:

-   crescimento regional;
-   países com maior crescimento;
-   países em queda;
-   status de crescimento;
-   metas sugeridas;
-   taxa de meta sugerida;
-   gap para meta.

Uma regra de materialidade de `0,01%` da receita histórica é aplicada
aos rankings percentuais para reduzir distorções provocadas por bases
extremamente pequenas.

### 04 \| Stream Conversion

Analisa a relação entre streams e receita por gênero.

A página apresenta:

-   receita;
-   streams;
-   receita por stream equivalente;
-   gênero com melhor conversão válida;
-   downloads;
-   ranking de conversão por gênero;
-   relação entre streams e receita;
-   desempenho de monetização;
-   participação de receita por gênero.

Uma regra de materialidade de `0,01%` dos streams do contexto é aplicada
ao ranking executivo de conversão.

## Projeção de Receita para 2025

Inicialmente foram avaliados quatro modelos utilizando rolling temporal
backtest:

-   Naive Forecast;
-   Linear Regression;
-   Holt Linear Trend;
-   Holt Damped Trend.

O modelo Naive apresentou o menor WAPE médio no backtest. Entretanto,
sua projeção repetia o último valor observado para todos os meses
futuros.

Considerando o histórico anual limitado, a projeção executiva final foi
implementada em DAX utilizando seleção dinâmica entre CAGR e regressão
linear.

A lógica é:

``` text
Variação entre taxas anuais <= 5 p.p.
        |
        v
       CAGR

Variação entre taxas anuais > 5 p.p.
        |
        v
Regressão Linear
```

A metodologia completa está documentada em `docs/methodology.md`.

## Materialidade Analítica

Percentuais de crescimento podem ser matematicamente elevados quando a
base histórica é extremamente pequena.

Para reduzir distorções em rankings executivos, foram utilizadas regras
de materialidade.

### Crescimento por Gênero

O gênero deve representar pelo menos:

``` text
0,01% da receita do contexto no período anterior
```

### Crescimento Regional

O país deve representar pelo menos:

``` text
0,01% da receita global do período anterior
```

### Conversão de Streams

O gênero deve representar pelo menos:

``` text
0,01% dos streams do contexto atual
```

Essas regras não removem registros do dataset. Elas são aplicadas apenas
às métricas e rankings executivos que podem ser distorcidos por bases
imateriais.

## Filtros do Relatório

As páginas utilizam os seguintes filtros:

-   Store
-   Country
-   Genre

As medidas DAX preservam o contexto aplicável dos filtros e recalculam
os indicadores, rankings e classificações dinamicamente.

## Limitação Conhecida --- Receita por Stream

A mart final disponível calcula a relação:

``` text
Receita Total / Total de Streams
```

A fonte analítica final não preserva a receita exclusivamente atribuída
aos registros classificados como stream.

Portanto, a métrica deve ser interpretada como:

``` text
Receita por Stream Equivalente
```

e não como receita exclusiva de streaming por stream.

Uma evolução futura ideal deve derivar, ainda na granularidade
detalhada:

``` text
stream_revenue =
CASE WHEN is_stream THEN revenue ELSE 0 END

download_revenue =
CASE WHEN is_download THEN revenue ELSE 0 END
```

A implementação dessa evolução depende de novo acesso aos arquivos
detalhados de origem.

## Limitação de Reprodutibilidade da Fonte

Os arquivos `.gz` originais eram acessados através de URLs temporárias.

Essas URLs expiraram após o processamento inicial.

Por esse motivo, a ingestão completa da camada raw não pode ser
reproduzida atualmente sem novas URLs válidas.

O desenvolvimento analítico e o dashboard foram concluídos utilizando o
`mart_sales.parquet` gerado durante o processamento original.

## Documentação

A documentação detalhada está disponível em:

``` text
docs/
├── architecture.md
├── data_dictionary.md
├── methodology.md
└── dashboard_guide.md
```

### `architecture.md`

Descrição da arquitetura, camadas e fluxo de processamento.

### `data_dictionary.md`

Dicionário das dimensões, métricas e principais medidas analíticas.

### `methodology.md`

Detalhamento das metodologias de forecast, Opportunity Score, análise
regional e conversão de streams.

### `dashboard_guide.md`

Guia das páginas, KPIs, visuais e narrativa executiva do dashboard.

## Narrativa Analítica

O relatório foi estruturado para conduzir a análise da observação à
ação:

``` text
Performance de Receita
        |
        v
Oportunidades de Crescimento
        |
        v
Priorização Regional
        |
        v
Eficiência de Monetização
```

A solução combina engenharia de dados, modelagem analítica e business
intelligence para transformar um conjunto de dados de grande volume em
indicadores executivos e direcionamentos de negócio.


## Acesso ao resultado

O dashboard final pode ser acessado por meio do arquivo Power BI disponibilizado na Release `v1.0.0` deste repositório.

### Dashboard com dados carregados

1. Acesse a seção **Releases** do repositório.
2. Abra a Release **Revenue Growth Intelligence v1.0.0**.
3. Na seção **Assets**, baixe o arquivo `Revenue.Growth.Intelligence.pbix`.
4. Abra o arquivo utilizando o Power BI Desktop.

O arquivo PBIX contém o modelo analítico e os dados importados utilizados na construção do dashboard.

### Projeto Power BI versionável

A estrutura PBIP também está disponível em:

`powerbi/`

O projeto contém:

- `Revenue Growth Intelligence.Report/`
- `Revenue Growth Intelligence.SemanticModel/`
- `Revenue Growth Intelligence.pbip`

Os arquivos locais de cache do Power BI (`cache.abf` e `localSettings.json`) não são versionados.

Por esse motivo, o projeto PBIP pode ser aberto para inspeção da estrutura do relatório e do modelo semântico, enquanto o arquivo PBIX disponibilizado na Release deve ser utilizado para visualizar o dashboard final com os dados carregados.
