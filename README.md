# Case Técnico - ONErpm

## Sobre o projeto

Este repositório contém minha solução para o case técnico da ONErpm.

O objetivo foi analisar dados de consumo musical, identificar oportunidades de negócio e desenvolver um dashboard executivo em Power BI para apoiar a tomada de decisão.

---

## Ferramentas utilizadas

- Python
- DuckDB
- SQL
- Power BI
- DAX
- Excel
- Google Apresentações

---

## Fluxo do projeto

1. Download dos arquivos de dados (.gz)
2. Processamento e agregação dos dados com Python e DuckDB
3. Geração de arquivos Parquet otimizados
4. Modelagem dos dados no Power BI
5. Construção do dashboard e dos indicadores
6. Desenvolvimento da apresentação executiva com os principais insights

---

## Materiais do repositório

## Estrutura do repositório

```text
python/
    Scripts de processamento dos dados

excel/
    Arquivo de excel para cálculo da projeção
```

### Power BI

- [Dashboard Interativo](https://app.powerbi.com/view?r=eyJrIjoiZjM1YTdiYjctZjYyYS00YTJiLWIxNjUtYzY0OTNiZTIxNTYzIiwidCI6ImYxOTI4MzIwLWU0YjItNDNhZS05NTg3LWY0NzY2Zjg2YzEzYyJ9)

### Apresentação

- [Apresentação do Case Técnico](https://docs.google.com/presentation/d/1I8CfT7VvKXRk9JaavCxSSXxNJTn1FJIiWJhTQO1-PoY/edit?usp=sharing)



---

## Observações

A base de dados original continha mais de 2 bilhões de registros. Para otimizar o processamento, foi desenvolvido um pipeline em Python utilizando DuckDB para realizar a agregação dos dados antes da importação para o Power BI.

## Transformações realizadas

Durante a preparação dos dados, foram realizadas algumas transformações para facilitar a análise e tornar os indicadores mais representativos do negócio.

- Criação do campo "consumption_type" com o grupamento das lojas (DSPs) em categorias de **Streaming** e **Social Media**, permitindo comparar consumo voltado à monetização com consumo voltado à descoberta de conteúdo.
- Criação do campo "store_group", agrupando as diferentes strings do "Store" que se referem às mesmas lojas. Ex: Spotify Free -> Spotify
- Criação da dimensão **Region** a partir do código dos países, possibilitando análises tanto por país quanto por região.
- Criação do campo "main_genre" com o agrupamento do "genre" em grupos macro. Ex: Baile Funk -> Funk, Trap -> Hip Hop/Rap
- Agregação dos dados em diferentes granularidades (mensal e anual) para otimizar o desempenho do Power BI.
- Criação de uma tabela calendário para permitir análises temporais, comparações com o mesmo período do ano anterior (YoY) e indicadores acumulados.
- Desenvolvimento de parâmetros dinâmicos para alternar entre:
  - Receita, Streams e RPM;
  - Valores absolutos e percentuais;
  - Visualização por País ou Região.
- Criação de KPIs comparativos com o mesmo período do ano anterior e benchmark do mercado.