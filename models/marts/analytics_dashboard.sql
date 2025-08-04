{{ config(materialized='view') }}

-- Dashboard Analytics Consolidado
-- Combina todos os insights principais em uma única tabela

WITH projecao_2025 AS (
    SELECT 
        'Projeção 2025' as categoria,
        'CAGR' as metric,
        cagr_anual as valor,
        'Porcentagem' as unidade
    FROM {{ ref('projecao_2025') }}
    
    UNION ALL
    
    SELECT 
        'Projeção 2025' as categoria,
        'Receita 2025' as metric,
        projecao_2025 as valor,
        'USD' as unidade
    FROM {{ ref('projecao_2025') }}
),

estrategia_receita AS (
    SELECT 
        'Estratégia receita' as categoria,
        item as metric,
        eficiencia_receita_stream as valor,
        'Eficiência' as unidade
    FROM {{ ref('estrategia_receita') }}
    WHERE categoria = 'plataforma' AND classificacao_eficiencia = 'Alta eficiência'
    ORDER BY eficiencia_receita_stream DESC
    LIMIT 1
),

tendencias_regionais AS (
    SELECT 
        'Tendências regionais' as categoria,
        country_code as metric,
        crescimento_percentual as valor,
        'Crescimento %' as unidade
    FROM {{ ref('tendencias_regionais') }}
    WHERE classificacao_crescimento = 'Alto crescimento'
    ORDER BY crescimento_percentual DESC
    LIMIT 3
),

melhor_genero AS (
    SELECT 
        'Melhor Gênero' as categoria,
        item as metric,
        eficiencia_receita_stream as valor,
        'Eficiência' as unidade
    FROM {{ ref('estrategia_receita') }}
    WHERE categoria = 'genero' AND classificacao_eficiencia = 'Média eficiência'
    ORDER BY eficiencia_receita_stream DESC
    LIMIT 1
)

SELECT * FROM projecao_2025
UNION ALL
SELECT * FROM estrategia_receita
UNION ALL
SELECT * FROM tendencias_regionais
UNION ALL
SELECT * FROM melhor_genero
ORDER BY categoria, metric 