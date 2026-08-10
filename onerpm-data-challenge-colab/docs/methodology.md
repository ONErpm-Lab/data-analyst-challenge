# Metodologia

## Objetivo

A solução analítica foi desenvolvida para responder quatro perguntas de
negócio:

1.  Qual a projeção de crescimento esperado para 2025?
2.  Qual a melhor estratégia para potencializar o aumento de receita?
3.  Quais regiões apresentam tendência e potencial de crescimento ou
    queda?
4.  Qual gênero apresenta a melhor relação entre streams e receita?

## Projeção de Receita para 2025

### Avaliação Inicial de Modelos

A série mensal possuía 35 observações válidas entre `Jan/2022` e
`Nov/2024`.

Foram avaliados:

-   Naive Forecast;
-   Linear Regression;
-   Holt Linear Trend;
-   Holt Damped Trend.

Foi utilizado rolling temporal backtest.

O modelo Naive apresentou o menor WAPE médio entre os modelos avaliados.
Entretanto, sua projeção repetia o último valor mensal observado em
todos os meses futuros.

Apesar do desempenho estatístico no backtest limitado, esse
comportamento foi considerado pouco representativo para uma projeção
executiva de crescimento.

### Decisão Final

A projeção de 2025 foi implementada em DAX com seleção dinâmica entre
CAGR e regressão linear.

A decisão considera o histórico anual limitado a três anos.

São calculadas as receitas de 2022, 2023 e 2024 e suas respectivas taxas
de crescimento.

A diferença absoluta entre as taxas anuais é utilizada como indicador de
estabilidade.

``` text
Variação <= 5 pontos percentuais
        |
        v
       CAGR

Variação > 5 pontos percentuais
        |
        v
Regressão Linear
```

Quando o comportamento de crescimento é relativamente estável,
utiliza-se CAGR.

Quando existe maior variação entre as taxas, utiliza-se regressão
linear.

### Interpretação

A seleção dinâmica é uma abordagem pragmática para uma série histórica
curta.

Com maior histórico, seria possível avaliar modelos como:

-   suavização exponencial sazonal;
-   SARIMA;
-   Prophet;
-   regressão polinomial;
-   modelos de machine learning para séries temporais.

A projeção deve ser interpretada como instrumento de planejamento
executivo e não como previsão causal de receita.

## Metodologia de Estratégia de Crescimento

### Opportunity Score

O score combina:

``` text
40% Crescimento
30% Monetização
30% Participação na Receita
```

Como os indicadores possuem escalas diferentes, cada componente é
normalizado entre 0 e 1.

O crescimento recebe maior peso por estar diretamente relacionado ao
objetivo de expansão de receita.

### Materialidade do Crescimento por Gênero

Bases históricas muito pequenas podem produzir crescimentos percentuais
extremos.

Um gênero participa da análise estratégica de crescimento quando sua
receita no período anterior representa pelo menos:

``` text
0,01% da receita do contexto no período anterior
```

A regra não remove o gênero da base. Apenas impede que uma base
historicamente imaterial distorça o ranking executivo.

### Quadrantes Estratégicos

#### Stars

``` text
Crescimento >= 5%
Monetização >= benchmark dos gêneros
```

Direção recomendada:

``` text
Expandir investimento e escala
```

#### Growth Opportunity

``` text
Crescimento >= 5%
Monetização < benchmark
```

Direção recomendada:

``` text
Melhorar monetização dos streams
```

#### Monetization Opportunity

``` text
Crescimento < 5%
Monetização >= benchmark
```

Direção recomendada:

``` text
Recuperar crescimento preservando eficiência de monetização
```

#### Low Priority

``` text
Crescimento < 5%
Monetização < benchmark
```

Direção recomendada:

``` text
Revisar alocação e priorizar oportunidades mais fortes
```

## Metodologia Regional

### Período Comparável

O crescimento regional compara:

``` text
Jan-Nov/2024
vs.
Jan-Nov/2023
```

Isso evita comparar um ano incompleto com um ano completo.

### Materialidade Regional

Um país participa do ranking percentual quando sua receita no período
anterior representa pelo menos:

``` text
0,01% da receita global do período anterior
```

A regra reduz distorções causadas por países com receita histórica
próxima de zero.

### Status de Crescimento

``` text
>= 10%       -> Forte Crescimento
>= 5%        -> Crescimento
<= -5%       -> Queda
Demais       -> Estável
Sem base     -> Base Insuficiente
```

### Metas Regionais

As metas sugeridas são aplicadas sobre a receita do período atual
comparável.

``` text
Meta Sugerida =
Receita Jan-Nov/2024
x
(1 + Taxa Meta Sugerida)
```

O gap é calculado como:

``` text
Meta Sugerida - Receita Jan-Nov/2024
```

As metas são recomendações analíticas e não representam orçamento
corporativo aprovado.

## Metodologia de Conversão de Streams

### Questão de Negócio

O objetivo é identificar gêneros com melhor relação entre streams e
receita.

A mart disponível permite calcular:

``` text
Receita Total / Total de Streams
```

Essa métrica é tratada como Receita por Stream Equivalente.

### Materialidade

Gêneros de volume muito baixo podem apresentar razões elevadas e pouco
relevantes para decisão executiva.

Um gênero participa do ranking quando representa pelo menos:

``` text
0,01% dos streams do contexto atual
```

### Limitação de Atribuição

A mart final não preserva receita exclusivamente associada a streaming.

Portanto:

``` text
Receita Total / Streams
```

não equivale a:

``` text
Receita de Streaming / Streams
```

Uma implementação futura ideal deve derivar, antes da agregação:

``` text
stream_revenue =
CASE WHEN is_stream THEN revenue ELSE 0 END

download_revenue =
CASE WHEN is_download THEN revenue ELSE 0 END
```

Essa evolução depende do acesso à fonte detalhada original.

## Princípios Analíticos

-   comparar períodos temporais equivalentes;
-   reduzir distorções de bases imateriais;
-   normalizar indicadores antes de combinar escalas;
-   preservar o contexto dos filtros de Store, Country e Genre;
-   distinguir correção matemática de relevância executiva;
-   documentar limitações decorrentes da granularidade da fonte.
