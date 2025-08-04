{{ config(materialized='table') }}

WITH receita_mensal_pais AS (
    SELECT 
        country_code,
        EXTRACT(YEAR FROM date) as ano,
        EXTRACT(MONTH FROM date) as mes,
        SUM(revenue) as receita_mensal,
        SUM(CASE WHEN is_stream = TRUE THEN quantity ELSE 0 END) as streams_mensal,
        SUM(CASE WHEN is_download = TRUE THEN quantity ELSE 0 END) as downloads_mensal,
        COUNT(*) as total_registros
    FROM {{ ref('stg_onerpm_raw') }}
    WHERE country_code IS NOT NULL
    GROUP BY country_code, EXTRACT(YEAR FROM date), EXTRACT(MONTH FROM date)
),
primeiro_ultimo_mes AS (
    SELECT 
        country_code,
        MIN(ano * 100 + mes) as primeiro_mes,
        MAX(ano * 100 + mes) as ultimo_mes
    FROM receita_mensal_pais
    GROUP BY country_code
),
crescimento_pais AS (
    SELECT 
        p.country_code,
        p.primeiro_mes,
        p.ultimo_mes,
        SUM(CASE WHEN r.ano * 100 + r.mes = p.primeiro_mes THEN r.receita_mensal ELSE 0 END) as receita_primeiro_mes,
        SUM(CASE WHEN r.ano * 100 + r.mes = p.ultimo_mes THEN r.receita_mensal ELSE 0 END) as receita_ultimo_mes,
        SUM(r.receita_mensal) as receita_total_periodo,
        SUM(r.streams_mensal) as streams_total_periodo,
        SUM(r.downloads_mensal) as downloads_total_periodo,
        SUM(r.total_registros) as total_registros_periodo
    FROM primeiro_ultimo_mes p
    JOIN receita_mensal_pais r ON p.country_code = r.country_code
    GROUP BY p.country_code, p.primeiro_mes, p.ultimo_mes
),
tendencias AS (
    SELECT 
        country_code,
        primeiro_mes,
        ultimo_mes,
        receita_primeiro_mes,
        receita_ultimo_mes,
        receita_total_periodo,
        streams_total_periodo,
        downloads_total_periodo,
        total_registros_periodo,
        CASE 
            WHEN receita_primeiro_mes > 0 THEN 
                ((receita_ultimo_mes - receita_primeiro_mes) / receita_primeiro_mes) * 100
            WHEN receita_ultimo_mes > 0 THEN 
                1000  -- Novo mercado
            ELSE 
                0  -- Sem atividade
        END as crescimento_percentual,
        CASE 
            WHEN receita_primeiro_mes > 0 THEN 'Crescimento normal'
            WHEN receita_ultimo_mes > 0 THEN 'Novo mercado'
            ELSE 'Sem atividade'
        END as tipo_crescimento
    FROM crescimento_pais
)
SELECT 
    'tendencias_regionais' as tipo_analise,
    country_code,
    primeiro_mes,
    ultimo_mes,
    receita_primeiro_mes,
    receita_ultimo_mes,
    receita_total_periodo,
    streams_total_periodo,
    downloads_total_periodo,
    total_registros_periodo,
    crescimento_percentual,
    tipo_crescimento,
    CASE 
        WHEN crescimento_percentual > 50 THEN 'Alto crescimento'
        WHEN crescimento_percentual > 20 THEN 'Crescimento moderado'
        WHEN crescimento_percentual > 0 THEN 'Crescimento baixo'
        WHEN crescimento_percentual = 1000 THEN 'Novo mercado'
        ELSE 'Sem crescimento'
    END as classificacao_crescimento,
    CASE 
        WHEN receita_total_periodo > 10000000 THEN 'Mercado grande'
        WHEN receita_total_periodo > 1000000 THEN 'Mercado médio'
        ELSE 'Mercado pequeno'
    END as tamanho_mercado,
    CURRENT_TIMESTAMP as data_criacao
FROM tendencias
ORDER BY receita_total_periodo DESC 