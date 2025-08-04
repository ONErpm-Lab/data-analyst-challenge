-- =====================================================
-- QUERIES BÁSICAS - RESPOSTAS ÀS 4 PERGUNTAS DO DESAFIO
-- =====================================================

-- PERGUNTA 1: Qual a projeção de crescimento esperado para 2025?
SELECT 
    'PERGUNTA 1: Projeção 2025' as pergunta,
    cagr_anual,
    projecao_2025,
    classificacao_crescimento,
    'RESPOSTA: CAGR de ' || ROUND(cagr_anual * 100, 1) || '% - ' || classificacao_crescimento || 
    '. Projeção 2025: $' || ROUND(projecao_2025, 0) as resposta
FROM projecao_2025;

-- PERGUNTA 2: Qual seria a melhor estatégia para potencializar o aumento de receita?
SELECT 
    'PERGUNTA 2: Melhor estratégia' as pergunta,
    item as plataforma,
    eficiencia_receita_stream,
    classificacao_eficiencia,
    'RESPOSTA: ' || item || ' é a plataforma mais eficiente (' || 
    ROUND(eficiencia_receita_stream, 6) || ') - ' || classificacao_eficiencia as resposta
FROM estrategia_receita 
WHERE categoria = 'plataforma' 
ORDER BY eficiencia_receita_stream DESC 
LIMIT 1;

-- PERGUNTA 3: Conseguimos identificar alguma região com tendência de crescimento e queda?
SELECT 
    'PERGUNTA 3: Tendências regionais' as pergunta,
    country_code,
    crescimento_percentual,
    classificacao_crescimento,
    'RESPOSTA: ' || country_code || ' tem ' || ROUND(crescimento_percentual, 1) || 
    '% de crescimento - ' || classificacao_crescimento as resposta
FROM tendencias_regionais 
WHERE crescimento_percentual > 0 
ORDER BY crescimento_percentual DESC 
LIMIT 3;

-- PERGUNTA 4: Qual gênero tem a melhor conversão de streams x receita?
SELECT 
    'PERGUNTA 4: Melhor gênero' as pergunta,
    item as genero,
    eficiencia_receita_stream,
    classificacao_eficiencia,
    'RESPOSTA: ' || item || ' tem melhor conversão (' || 
    ROUND(eficiencia_receita_stream, 6) || ') - ' || classificacao_eficiencia as resposta
FROM estrategia_receita 
WHERE categoria = 'genero' 
ORDER BY eficiencia_receita_stream DESC 
LIMIT 1; 