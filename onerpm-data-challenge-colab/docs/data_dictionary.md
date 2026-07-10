# Dicionário de Dados

## Dataset Analítico

Dataset principal:

``` text
mart_sales
```

Granularidade:

``` text
Ano x Mês x Loja x País x Gênero
```

Cada linha representa a atividade agregada para uma combinação de ano,
mês, loja, país e gênero.

## Dimensões

  Campo            Tipo      Descrição
  ---------------- --------- ---------------------------------
  `year`           Inteiro   Ano calendário.
  `month`          Inteiro   Número do mês, de 1 a 12.
  `store`          Texto     Plataforma ou fonte de receita.
  `country_code`   Texto     Código do país.
  `genre_id`       Inteiro   Identificador do gênero.
  `genre_name`     Texto     Nome do gênero.

## Métricas Base

  -----------------------------------------------------------------------
  Campo                               Descrição
  ----------------------------------- -----------------------------------
  `sales`                             Quantidade de registros da origem
                                      agregados no grão da mart.

  `quantity`                          Soma da quantidade da origem.

  `revenue`                           Receita total.

  `streams`                           Quantidade associada a registros
                                      classificados como stream.

  `downloads`                         Quantidade associada a registros
                                      classificados como download.
  -----------------------------------------------------------------------

## Métricas Derivadas da Mart

  ----------------------------------------------------------------------------
  Campo                        Fórmula                 Descrição
  ---------------------------- ----------------------- -----------------------
  `avg_ticket`                 `revenue / sales`       Receita média por
                                                       registro agregado.

  `avg_revenue_per_stream`     `revenue / streams`     Receita por stream
                                                       equivalente.

  `avg_revenue_per_download`   `revenue / downloads`   Receita por download
                                                       equivalente.

  `avg_revenue_per_sale`       `revenue / quantity`    Receita média por
                                                       unidade de quantidade.
  ----------------------------------------------------------------------------

## Principais Medidas do Power BI

### Receita

Soma de `mart_sales[revenue]` no contexto atual de filtros.

### Streams

Soma de `mart_sales[streams]`.

### Downloads

Soma de `mart_sales[downloads]`.

### Receita por Stream

Fórmula conceitual:

``` text
Receita Total / Total de Streams
```

Devido à granularidade disponível na mart final, a interpretação mais
precisa é `Receita por Stream Equivalente`.

### Receita por Download

Fórmula conceitual:

``` text
Receita Total / Total de Downloads
```

A mesma limitação de atribuição de receita se aplica.

## Inteligência Temporal

### Crescimento YoY Atual

Compara períodos equivalentes:

``` text
Jan-Nov/2024
vs.
Jan-Nov/2023
```

Fórmula conceitual:

``` text
(Receita Atual - Receita Anterior)
/
Receita Anterior
```

### Receita Período Atual

Receita de `Jan-Nov/2024`.

### Receita PY Atual

Receita de `Jan-Nov/2023`.

## Medidas de Forecast

### Projeção Receita 2025

Projeção baseada em seleção dinâmica entre CAGR e regressão linear.

### Crescimento Projetado 2025

Taxa de crescimento da projeção de 2025 em relação à base anualizada de
2024.

### Modelo Forecast Selecionado

Identifica o método selecionado pela lógica DAX.

### Justificativa Modelo Forecast

Apresenta a justificativa textual da seleção do modelo.

## Medidas Estratégicas

### Receita Share

Participação da receita do gênero no contexto selecionado.

### Opportunity Score

Índice de 0 a 100 composto por:

``` text
40% Crescimento YoY
30% Monetização por Stream
30% Participação na Receita
```

Os componentes são normalizados antes da aplicação dos pesos.

### Quadrante Estratégico

  Quadrante                  Interpretação
  -------------------------- ----------------------------------------
  Stars                      Alto crescimento e alta monetização.
  Growth Opportunity         Alto crescimento e menor monetização.
  Monetization Opportunity   Menor crescimento e alta monetização.
  Low Priority               Menor crescimento e menor monetização.

### Estratégia Recomendada

Recomendação textual derivada do quadrante estratégico.

## Medidas Regionais

### Crescimento YoY Regional Válido

Exige que a receita do país no período anterior represente pelo menos
`0,01%` da receita global do mesmo contexto.

A regra reduz distorções percentuais causadas por bases históricas
extremamente pequenas.

### Status Crescimento

  Crescimento             Status
  ----------------------- -------------------
  `>= 10%`                Forte Crescimento
  `>= 5%`                 Crescimento
  `<= -5%`                Queda
  Entre `-5%` e `5%`      Estável
  Sem comparação válida   Base Insuficiente

### Meta Sugerida

``` text
Receita Período Atual x (1 + Taxa Meta Sugerida)
```

### Gap para Meta

``` text
Meta Sugerida - Receita Período Atual
```

### Rank Crescimento País

Ranking decrescente dos países por crescimento regional válido.

### Rank Queda País

Ranking crescente dos países por crescimento regional válido.

## Materialidade de Conversão

### Receita por Stream Válida

Um gênero participa do ranking executivo de conversão quando representa
pelo menos:

``` text
0,01% dos streams do contexto atual
```

### Top Genre Conversion

Retorna o gênero com maior Receita por Stream Válida.

## Limitação da Métrica de Conversão

A mart final não preserva a receita exclusivamente atribuída a registros
de stream.

Portanto:

``` text
Receita por Stream = Receita Total / Streams
```

não deve ser interpretada como receita exclusiva de streaming por
stream.

O nome conceitualmente mais preciso é:

``` text
Receita por Stream Equivalente
```

Uma evolução futura deve derivar `stream_revenue` e `download_revenue`
antes da agregação.
