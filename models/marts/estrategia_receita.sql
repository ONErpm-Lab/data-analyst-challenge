{{ config(materialized='table') }}

WITH eficiencia_plataforma AS (
    SELECT 
        store,
        SUM(CASE WHEN is_stream = TRUE THEN quantity ELSE 0 END) as total_streams,
        SUM(revenue) as receita_total,
        CASE 
            WHEN SUM(CASE WHEN is_stream = TRUE THEN quantity ELSE 0 END) > 0 
            THEN SUM(revenue) / SUM(CASE WHEN is_stream = TRUE THEN quantity ELSE 0 END)
            ELSE 0 
        END as eficiencia_receita_stream,
        COUNT(*) as total_registros
    FROM {{ ref('stg_onerpm_raw') }}
    WHERE is_stream = TRUE
    GROUP BY store
    HAVING SUM(CASE WHEN is_stream = TRUE THEN quantity ELSE 0 END) >= 1000
),
eficiencia_genero AS (
    SELECT 
        genre_name,
        SUM(CASE WHEN is_stream = TRUE THEN quantity ELSE 0 END) as total_streams,
        SUM(revenue) as receita_total,
        CASE 
            WHEN SUM(CASE WHEN is_stream = TRUE THEN quantity ELSE 0 END) > 0 
            THEN SUM(revenue) / SUM(CASE WHEN is_stream = TRUE THEN quantity ELSE 0 END)
            ELSE 0 
        END as eficiencia_receita_stream,
        COUNT(*) as total_registros,
        COUNT(DISTINCT store) as plataformas_unicas,
        COUNT(DISTINCT country_code) as paises_unicos
    FROM {{ ref('stg_onerpm_raw') }}
    WHERE is_stream = TRUE AND genre_name IS NOT NULL
    GROUP BY genre_name
    HAVING SUM(CASE WHEN is_stream = TRUE THEN quantity ELSE 0 END) >= 100
),
eficiencia_genero_plataforma AS (
    SELECT 
        genre_name,
        store,
        SUM(CASE WHEN is_stream = TRUE THEN quantity ELSE 0 END) as total_streams,
        SUM(revenue) as receita_total,
        CASE 
            WHEN SUM(CASE WHEN is_stream = TRUE THEN quantity ELSE 0 END) > 0 
            THEN SUM(revenue) / SUM(CASE WHEN is_stream = TRUE THEN quantity ELSE 0 END)
            ELSE 0 
        END as eficiencia_receita_stream,
        COUNT(*) as total_registros
    FROM {{ ref('stg_onerpm_raw') }}
    WHERE is_stream = TRUE AND genre_name IS NOT NULL AND store IS NOT NULL
    GROUP BY genre_name, store
    HAVING SUM(CASE WHEN is_stream = TRUE THEN quantity ELSE 0 END) >= 100
),
sazonalidade AS (
    SELECT 
        EXTRACT(MONTH FROM date) as mes,
        EXTRACT(YEAR FROM date) as ano,
        SUM(revenue) as receita_total,
        SUM(CASE WHEN is_stream = TRUE THEN quantity ELSE 0 END) as total_streams,
        COUNT(*) as total_registros
    FROM {{ ref('stg_onerpm_raw') }}
    GROUP BY EXTRACT(MONTH FROM date), EXTRACT(YEAR FROM date)
)
-- Eficiência por Plataforma
SELECT 
    'estrategia_receita' as tipo_analise,
    'plataforma' as categoria,
    store as item,
    total_streams,
    receita_total,
    eficiencia_receita_stream,
    total_registros,
    NULL as plataformas_unicas,
    NULL as paises_unicos,
    NULL as nivel_confiabilidade,
    CASE 
        WHEN eficiencia_receita_stream > 0.05 THEN 'Alta eficiência'
        WHEN eficiencia_receita_stream > 0.01 THEN 'Média eficiência'
        ELSE 'Baixa eficiência'
    END as classificacao_eficiencia,
    CURRENT_TIMESTAMP as data_criacao
FROM eficiencia_plataforma

UNION ALL

-- Eficiência por Gênero (Geral)
SELECT 
    'estrategia_receita' as tipo_analise,
    'genero' as categoria,
    genre_name as item,
    total_streams,
    receita_total,
    eficiencia_receita_stream,
    total_registros,
    plataformas_unicas,
    paises_unicos,
    CASE 
        WHEN total_streams >= 10000 THEN 'Alta confiabilidade'
        WHEN total_streams >= 1000 THEN 'Média confiabilidade'
        ELSE 'Baixa confiabilidade'
    END as nivel_confiabilidade,
    CASE 
        WHEN eficiencia_receita_stream > 0.005 THEN 'Alta eficiência'
        WHEN eficiencia_receita_stream > 0.001 THEN 'Média eficiência'
        ELSE 'Baixa eficiência'
    END as classificacao_eficiencia,
    CURRENT_TIMESTAMP as data_criacao
FROM eficiencia_genero

UNION ALL

-- Eficiência por Gênero e Plataforma
SELECT 
    'estrategia_receita' as tipo_analise,
    'genero_plataforma' as categoria,
    CONCAT(genre_name, ' - ', store) as item,
    total_streams,
    receita_total,
    eficiencia_receita_stream,
    total_registros,
    1 as plataformas_unicas,
    1 as paises_unicos,
    CASE 
        WHEN total_streams >= 10000 THEN 'Alta confiabilidade'
        WHEN total_streams >= 1000 THEN 'Média confiabilidade'
        ELSE 'Baixa confiabilidade'
    END as nivel_confiabilidade,
    CASE 
        WHEN eficiencia_receita_stream > 0.005 THEN 'Alta eficiência'
        WHEN eficiencia_receita_stream > 0.001 THEN 'Média eficiência'
        ELSE 'Baixa eficiência'
    END as classificacao_eficiencia,
    CURRENT_TIMESTAMP as data_criacao
FROM eficiencia_genero_plataforma

UNION ALL

-- Sazonalidade
SELECT 
    'estrategia_receita' as tipo_analise,
    'sazonalidade' as categoria,
    CAST(mes AS VARCHAR) as item,
    total_streams,
    receita_total,
    0 as eficiencia_receita_stream,
    total_registros,
    NULL as plataformas_unicas,
    NULL as paises_unicos,
    NULL as nivel_confiabilidade,
    CASE 
        WHEN mes IN (12, 1, 2) THEN 'Alta temporada'
        WHEN mes IN (6, 7, 8) THEN 'Média temporada'
        ELSE 'Baixa temporada'
    END as classificacao_eficiencia,
    CURRENT_TIMESTAMP as data_criacao
FROM sazonalidade 