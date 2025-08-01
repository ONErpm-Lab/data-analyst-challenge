# ONErpm Data Analyst Challenge

## 📊 Coleta de Dados

Para este projeto, o primeiro passo foi a ingestão e transformação inicial dos dados brutos fornecidos em arquivos `.gz`, utilizando **Python** com **PySpark**, e salvando-os em um banco de dados **PostgreSQL** para análises futuras com DBT e visualização com Power BI.

A lógica de coleta está implementada no diretório `/pipeline`.

### 🔍 Estratégia de Amostragem

Devido a limitações de armazenamento local, foi aplicada uma estratégia de amostragem aleatória com o método `.sample(fraction=0.1, seed=42)` do PySpark, garantindo uma extração de 10% dos dados de cada arquivo de forma reprodutível (Isso foi necessário por falta de recurso computacional localmente).

### 🐘 Armazenamento em PostgreSQL

Os dados tratados foram gravados em um banco de dados PostgreSQL, permitindo consultas otimizadas e integração com ferramentas como DBT para modelagem e transformação posterior.

### 🐳 Ambiente com Docker

A coleta foi realizada em um ambiente isolado com **Docker**, que incluiu:

- Container com a aplicação Python/PySpark
- Container para o banco de dados PostgreSQL

### 🔧 Como executar o pipeline de coleta

```bash

# Subir os containers postgres
docker-compose up --build

# Entrar no container para executar a coleta
docker-compose run spark-job bash

# dentro do container execute
spark-submit src/main.py
```

## 🏗️ Modelagem com DBT

Após a carga dos dados brutos no PostgreSQL, as análises foram estruturadas utilizando o **DBT (Data Build Tool)**, com foco em manter um fluxo analítico organizado e modular.

O projeto DBT está localizado em:


### 🧱 Estrutura de Camadas

Foi utilizada uma arquitetura em camadas seguindo boas práticas de engenharia de dados:

- `silver/`: contém os dados **raw**, extraídos diretamente dos arquivos `.gz` e carregados no PostgreSQL via PySpark. Não foram realizadas transformações nesta camada, mantendo os dados como originalmente recebidos. Apenas foram juntados os dados dos dois arquivos que estavam em tabelas diferente na camada **raw**.
- `gold/`: camada de **modelos analíticos** prontos para consumo por ferramentas de visualização como o Power BI. Todas as análises e métricas foram construídas nesta camada.

> Os modelos estão na pasta `dbt/models/gold/`.

### ▶️ Como executar o projeto DBT

Antes de executar o DBT, verifique se o banco de dados está populado e rodando via Docker.

1. Acesse o container (ou seu ambiente local com DBT instalado):
```bash
docker-compose run dbt-job bash
cd app/app/dbt/

# Instalar dependências (se houver)
dbt deps

# Verificar status do projeto
dbt debug

# Compilar e executar os modelos
dbt run

# Visualizar os modelos gerados
dbt docs generate
dbt docs serve
```

## ❓ Respostas às Perguntas de Negócio

As análises para responder às perguntas do desafio foram implementadas diretamente na camada `gold/` do projeto DBT. Abaixo, explicamos como cada uma foi tratada.

---

### 1. Qual a projeção de crescimento esperado para 2025?

Para estimar a projeção de crescimento de receita em 2025, foi criado o modelo: `dbt/models/gold/agg_revenue_by_year.sql`.

Este modelo realiza a agregação da receita por ano, permitindo observar a evolução ao longo do tempo e projetar tendências de crescimento com base nos dados históricos de 2022 a 2024.

**Observação:** embora o DBT não seja ideal para modelos preditivos, como regressões lineares, ele é eficaz para preparar e expor os dados necessários para análises projetivas via ferramentas externas como Power BI ou Python.

---

### 2. Qual seria a melhor estratégia para potencializar o aumento de receita?

Para esta análise, foi desenvolvido o modelo: `dbt/models/gold/revenue_by_channel_and_genre.sql`.

Este modelo cruza a receita com as variáveis **tipo de venda (stream/download)**, **loja** e **gênero musical**, fornecendo uma visão detalhada de performance por canal.

Com base neste modelo, é possível identificar:

- Quais canais (streaming vs. download) são mais lucrativos
- Quais lojas (Spotify, YouTube, Amazon, Facebook) contribuem com maior receita
- Quais gêneros musicais têm melhor conversão de streams em receita

Essas informações são fundamentais para direcionar ações estratégicas de marketing e distribuição, otimizando o crescimento de receita.

---

### 3. Conseguimos identificar alguma região que esteja com tendência e potencial de crescimento e de queda?

Para responder a essa pergunta, foi criado o modelo: dbt/models/gold/agg_country_monthly_growth.sql

Este modelo calcula a **variação percentual da receita mês a mês por país**, permitindo:

- Identificar **tendências de crescimento** ou queda de receita em diferentes regiões
- Monitorar sazonalidades ou anomalias locais
- Apoiar decisões estratégicas regionais com base em dados históricos de performance

A análise mensal por país é essencial para os Chefes Regionais acompanharem sua evolução e adaptarem metas e estratégias de forma direcionada.

---


### 4. Qual gênero tem a melhor conversão de streams x receita?

Para responder a essa pergunta, foi desenvolvido o modelo: dbt/models/gold/genre_stream_conversion.sql


Este modelo calcula a **receita média por stream (revenue per stream)** para cada gênero musical, permitindo identificar:

- Quais gêneros geram mais receita por unidade de stream
- O desempenho relativo entre gêneros populares versus nichados
- Oportunidades de investimento estratégico em gêneros mais rentáveis

Essa métrica é importante para entender **eficiência de monetização por gênero**, complementando análises de volume e receita total.

---

## 📊 Dashboard Executivo

### Visões Propostas

O dashboard foi idealizado para atender a dois perfis principais de usuários:

- **Visão Global para Diretores Executivos:** foco em métricas agregadas e tendências globais, como receita total, crescimento anual e canais mais lucrativos.
- **Visão Regional para Chefes Regionais:** foco em desempenho por país e metas regionais, com filtros por gênero e canal de distribuição.

### Métricas e Parâmetros Utilizados

- **Métricas principais:** receita total, número de streams (baseado na flag `is_stream`)
- **Parâmetros e dimensões:** lojas (Amazon, YouTube, Spotify, Facebook), países (`country_code`) e gêneros (`genre_name`)

### 🎨 Gráficos e Visualizações

> **Nota:** Por limitações técnicas, não foi possível montar o dashboard final no Power BI. Estou utilizando Linux e não tenho acesso ao Power BI Web, pois ele requer conta corporativa. No entanto, abaixo descrevo exatamente como eu estruturaria as visualizações:

- **Receita por Ano**
  - **Fonte:** `gold/agg_revenue_by_year.sql`
  - **Gráfico:** Linha ou barras, dependendo da estética do dashboard
  - **Objetivo:** Demonstrar crescimento ou retração da receita ao longo dos anos

- **Top Canais e Gêneros**
  - **Fonte:** `gold/revenue_by_channel_and_genre.sql`
  - **Gráfico:** Barra empilhada
  - **Objetivo:** Entender quais canais e gêneros têm maior participação na receita

- **Crescimento por País**
  - **Fonte:** `gold/agg_country_monthly_growth.sql`
  - **Gráfico:** Mapa interativo (se disponível) ou gráfico de linha com filtro por país
  - **Objetivo:** Visualizar tendências regionais e identificar áreas de crescimento ou queda

- **Conversão de Streams em Receita por Gênero**
  - **Fonte:** `gold/genre_stream_conversion.sql`
  - **Gráfico:** Tabela simples com colunas `genre_name` e `revenue_per_stream`
  - **Objetivo:** Analisar a eficiência de monetização de cada gênero

---

Mesmo sem o dashboard final renderizado, os modelos DBT foram preparados para fornecer todos os dados necessários para uma futura integração visual em Power BI ou outras ferramentas compatíveis.

Observação: tentei entregar o mais rapido possível uma solução, pois gostei muito da vaga, empresa e