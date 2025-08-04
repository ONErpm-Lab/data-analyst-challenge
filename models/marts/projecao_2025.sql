{{ config(materialized='table') }}

WITH receita_anual AS (
    SELECT 
        EXTRACT(YEAR FROM date) as ano,
        SUM(revenue) as receita_total,
        COUNT(*) as total_registros
    FROM {{ ref('stg_onerpm_raw') }}
    GROUP BY EXTRACT(YEAR FROM date)
    ORDER BY ano
),
cagr_calculo AS (
    SELECT 
        MIN(ano) as primeiro_ano,
        MAX(ano) as ultimo_ano,
        MIN(receita_total) as receita_primeiro_ano,
        MAX(receita_total) as receita_ultimo_ano,
        COUNT(*) as total_anos,
        SUM(receita_total) as receita_total_periodo
    FROM receita_anual
),
projecao AS (
    SELECT 
        primeiro_ano,
        ultimo_ano,
        receita_primeiro_ano,
        receita_ultimo_ano,
        total_anos,
        receita_total_periodo,
        CASE 
            WHEN receita_primeiro_ano > 0 THEN 
                POWER(receita_ultimo_ano / receita_primeiro_ano, 1.0 / (ultimo_ano - primeiro_ano)) - 1
            ELSE NULL 
        END as cagr_anual,
        CASE 
            WHEN receita_primeiro_ano > 0 THEN 
                receita_ultimo_ano * (1 + POWER(receita_ultimo_ano / receita_primeiro_ano, 1.0 / (ultimo_ano - primeiro_ano)) - 1)
            ELSE NULL 
        END as projecao_2025
    FROM cagr_calculo
)
SELECT 
    'projecao_2025' as tipo_analise,
    primeiro_ano,
    ultimo_ano,
    receita_primeiro_ano,
    receita_ultimo_ano,
    total_anos,
    receita_total_periodo,
    cagr_anual,
    projecao_2025,
    projecao_2025 - receita_ultimo_ano as crescimento_esperado,
    CASE 
        WHEN cagr_anual > 0.1 THEN 'Crescimento alto'
        WHEN cagr_anual > 0.05 THEN 'Crescimento moderado'
        WHEN cagr_anual > 0 THEN 'Crescimento baixo'
        ELSE 'Sem crescimento'
    END as classificacao_crescimento,
    CURRENT_TIMESTAMP as data_criacao
FROM projecao 