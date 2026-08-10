# Guia do Dashboard

## Relatório

Projeto Power BI:

``` text
Revenue Growth Intelligence.pbip
```

Públicos principais:

-   Diretores Executivos;
-   Chefes Regionais.

O relatório possui quatro páginas.

## 01 \| Executive Overview

### Objetivo

Apresentar uma visão executiva global da receita, streams, crescimento
histórico e projeção para 2025.

### KPIs

-   Total Revenue
-   Total Streams
-   YoY Growth
-   Forecast Revenue 2025
-   Forecast Growth 2025

### Monthly Revenue Trend

Apresenta a evolução mensal da receita e a comparação com o período do
ano anterior.

### Top Revenue Sources by Store

Exibe as cinco lojas com maior receita.

Objetivo:

-   identificar as principais fontes de receita;
-   avaliar concentração por plataforma.

### Revenue by Country

Apresenta a distribuição geográfica da receita.

### Revenue by Genre

Exibe os 15 gêneros de maior receita.

### Filtros

-   Store
-   Country
-   Genre

## 02 \| Growth Strategy

### Objetivo

Identificar oportunidades de crescimento e recomendar ações
estratégicas.

### KPIs

-   Top Opportunity
-   Opportunity Score
-   Opportunity Classification
-   Strategic Quadrant
-   Recommended Strategy

### Growth vs. Monetization Opportunity Matrix

Gráfico de dispersão:

``` text
Eixo X = Receita por Stream Equivalente
Eixo Y = Crescimento YoY Oportunidade Válido
```

Cada ponto representa um gênero.

### Opportunity Ranking

Ordena os gêneros pelo Opportunity Score:

``` text
40% Crescimento
30% Monetização
30% Receita Share
```

Gêneros sem comparação histórica válida são excluídos do score.

### Strategic Opportunity Matrix

Apresenta:

-   Genre
-   Opportunity Score
-   Opportunity Classification
-   Strategic Quadrant
-   Recommended Strategy

O objetivo é converter indicadores analíticos em direcionamentos de
negócio.

## 03 \| Regional Performance

### Objetivo

Identificar países em crescimento ou queda e apresentar metas regionais
sugeridas.

### KPIs

-   Current Period Revenue
-   YoY Growth
-   Growth Status
-   Suggested Target
-   Suggested Target Rate

### Período Comparável

``` text
Jan-Nov/2024
vs.
Jan-Nov/2023
```

### Regional Growth vs. Revenue

``` text
Eixo X = Receita
Eixo Y = Crescimento YoY Regional Válido
Tamanho = Streams
Detalhe = Country
```

Permite identificar países financeiramente relevantes em crescimento ou
queda.

### Top Growing Countries

Exibe os dez países com maior crescimento positivo válido.

### Countries in Decline

Exibe os dez países com maior queda válida.

### Regional Target Performance

Apresenta:

-   Country
-   Receita Período Atual
-   Crescimento YoY Regional Válido
-   Status Crescimento
-   Meta Sugerida
-   Taxa Meta Sugerida
-   Gap para Meta

O visual fornece aos Chefes Regionais uma visão operacional de
desempenho e metas.

## 04 \| Stream Conversion

### Objetivo

Identificar gêneros com melhor relação entre receita e streams em uma
escala material de volume.

### KPIs

-   Total Revenue
-   Total Streams
-   Revenue per Stream Equivalent
-   Best Converting Genre
-   Total Downloads

### Revenue per Stream Equivalent by Genre

Exibe os 15 gêneros com maior Receita por Stream Válida.

Para participar do ranking, o gênero deve representar pelo menos:

``` text
0,01% dos streams do contexto atual
```

### Streams vs. Revenue by Genre

``` text
Eixo X = Streams
Eixo Y = Receita
Tamanho = Receita por Stream Equivalente
Detalhe = Genre
```

Permite comparar escala de streams, receita e eficiência relativa.

### Genre Monetization Performance

Apresenta:

-   Genre
-   Receita
-   Streams
-   Receita por Stream Equivalente
-   Downloads
-   Receita por Download Equivalente

### Revenue Share by Genre

Treemap dos 15 gêneros de maior receita.

## Comportamento dos Filtros

As páginas utilizam:

``` text
Store
Country
Genre
```

As medidas analíticas preservam o contexto relevante dos filtros.

A seleção de um país, loja ou gênero recalcula os indicadores
aplicáveis, rankings e classificações.

## Sequência Recomendada de Apresentação

``` text
01 | Executive Overview
        |
        v
O que está acontecendo?

02 | Growth Strategy
        |
        v
Onde estão as oportunidades e qual ação é recomendada?

03 | Regional Performance
        |
        v
Quais países crescem ou caem e quais metas são sugeridas?

04 | Stream Conversion
        |
        v
Quais gêneros apresentam melhor relação entre receita e streams?
```

## Narrativa Executiva

O relatório foi estruturado para evoluir da observação para a ação:

``` text
Performance de Receita
        |
        v
Oportunidade de Crescimento
        |
        v
Priorização Regional
        |
        v
Eficiência de Monetização
```

## Limitação Conhecida

A métrica de conversão utiliza:

``` text
Receita Total / Total de Streams
```

porque a mart final não preserva a receita exclusivamente atribuída aos
registros de stream.

A métrica deve ser interpretada como:

``` text
Revenue per Stream Equivalent
```

e não como receita exclusiva de streaming por stream.

O recálculo com receita exclusiva de streaming depende do acesso à fonte
detalhada original.
