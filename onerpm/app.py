import streamlit as st
import duckdb
import pandas as pd
import os
from datetime import datetime

# --- Configuração da página ---
st.set_page_config(
    page_title="🎶 ONErpm Dashboard",
    page_icon="",
    layout="wide"
)

# --- Conexão com DuckDB ---
con = duckdb.connect(os.path.join(os.path.dirname(__file__), "../onerpm.duckdb"))

# --- Carregar dados de base ---
@st.cache_data
def load_data():
    return con.execute("SELECT * FROM stg_onerpm_raw LIMIT 10000000").df()

df = load_data()

# --- Processar dados para o dashboard ---
@st.cache_data
def process_data(df):
    # Extrair ano e mês da data
    df['date'] = pd.to_datetime(df['date'])
    df['ano'] = df['date'].dt.year
    df['mes'] = df['date'].dt.month
    df['mes_nome'] = df['date'].dt.strftime('%B')
    
    # Converter revenue para USD se necessário
    df['revenue_usd'] = df['revenue']
    
    return df

df_processed = process_data(df)

# --- Sidebar com filtros ---
st.sidebar.header("Filtros")

# Filtros de data
anos = sorted(df_processed["ano"].dropna().unique())
ano_selecionado = st.sidebar.selectbox("Ano", anos, index=len(anos)-1)

meses = sorted(df_processed[df_processed["ano"] == ano_selecionado]["mes"].unique())
mes_selecionado = st.sidebar.selectbox("Mês", meses, index=len(meses)-1)

# Filtros de gênero e país
generos = sorted([x for x in df_processed["genre_name"].unique() if x is not None])
genero_selecionado = st.sidebar.multiselect("Gênero Musical", generos, default=generos[:5])

paises = sorted([x for x in df_processed["country_code"].unique() if x is not None])
pais_selecionado = st.sidebar.multiselect("País", paises, default=paises[:10])

# Filtros de tipo de transação
tipo_transacao = st.sidebar.multiselect(
    "Tipo de Transação",
    ["Streams", "Downloads"],
    default=["Streams", "Downloads"]
)

# --- Aplicar filtros ---
df_filtrado = df_processed[
    (df_processed["ano"] == ano_selecionado) &
    (df_processed["mes"] == mes_selecionado) &
    (df_processed["genre_name"].isin(genero_selecionado)) &
    (df_processed["country_code"].isin(pais_selecionado))
]

# --- Header principal ---
st.title("🎶 Dashboard ONErpm")
st.info("**Amostra de dados**: Este dashboard utiliza uma amostra de 10.000.000 registros para análise de big data")

# Adicionar avisos sobre limitações da amostra
with st.expander("Limitações da amostra", expanded=False):
    st.warning("""
    **Considerações importantes sobre a análise:**
    
    **Amostra de 10M registros** - os resultados podem não refletir a população completa
    """)

# --- Criar abas ---
tab1, tab2, tab3, tab4 = st.tabs([
    "Visão Executiva", 
    "Visão Regional", 
    "Análises Detalhadas",
    "Projeções & Estratégias"
])

# --- ABA 1: VISÃO EXECUTIVA ---
with tab1:
    st.header("Visão Executiva")
    st.subheader("KPIs Principais")
    
    # Calcular métricas principais
    total_revenue = df_filtrado['revenue'].sum()
    total_streams = df_filtrado[df_filtrado['is_stream'] == True]['quantity'].sum()
    total_downloads = df_filtrado[df_filtrado['is_download'] == True]['quantity'].sum()
    avg_revenue_per_stream = total_revenue / total_streams if total_streams > 0 else 0
    
    # Layout em colunas
    col1, col2, col3, col4 = st.columns(4)
    
    col1.metric("Receita Total", f"${total_revenue:,.2f}")
    col2.metric("Total de Streams", f"{total_streams:,.0f}")
    col3.metric("Downloads", f"{total_downloads:,.0f}")
    col4.metric("Receita/Stream", f"${avg_revenue_per_stream:.4f}")
    
    # Gráfico de evolução temporal - mostra tendências ao longo do tempo
    # Escolhi line chart porque é melhor para séries temporais
    st.subheader("Evolução da receita")
    
    # Agrupando por data para o gráfico
    receita_por_data = df_filtrado.groupby('date')['revenue'].sum().reset_index()
    
    # Criar gráfico
    import plotly.express as px
    fig = px.line(receita_por_data, x='date', y='revenue', 
                  title='Evolução da receita ao longo do tempo')
    fig.update_layout(xaxis_title="Data", yaxis_title="Receita (USD)")
    st.plotly_chart(fig, use_container_width=True)
    
    # Top 5 países por receita
    st.subheader("Top 5 países por receita")
    top_paises = df_filtrado.groupby('country_code')['revenue'].sum().sort_values(ascending=False).head(5)
    
    # Converter para DataFrame para compatibilidade com Plotly
    df_paises = pd.DataFrame({
        'country_code': top_paises.index,
        'revenue': top_paises.values
    })
    
    fig_paises = px.bar(df_paises, x='country_code', y='revenue',
                        title='Top 5 países por receita')
    fig_paises.update_layout(xaxis_title="País", yaxis_title="Receita (USD)")
    st.plotly_chart(fig_paises, use_container_width=True)

# --- ABA 2: VISÃO REGIONAL ---
with tab2:
    st.header("Visão Regional")
    
    # Selecionar região
    regiao_selecionada = st.selectbox(
        "Selecionar região",
        df_filtrado['country_code'].unique()
    )
    
    # Filtrar dados da região
    df_regional = df_filtrado[df_filtrado['country_code'] == regiao_selecionada]
    
    if not df_regional.empty:
        # Métricas da região
        col1, col2, col3, col4 = st.columns(4)
        
        receita_regional = df_regional['revenue'].sum()
        streams_regional = df_regional[df_regional['is_stream'] == True]['quantity'].sum()
        downloads_regional = df_regional[df_regional['is_download'] == True]['quantity'].sum()
        eficiencia_regional = receita_regional / streams_regional if streams_regional > 0 else 0
        
        col1.metric(f"Receita {regiao_selecionada}", f"${receita_regional:,.2f}")
        col2.metric(f"Streams {regiao_selecionada}", f"{streams_regional:,.0f}")
        col3.metric(f"Downloads {regiao_selecionada}", f"{downloads_regional:,.0f}")
        col4.metric(f"Eficiência {regiao_selecionada}", f"${eficiencia_regional:.6f}")
        
        # Gráfico de receita por gênero na região
        st.subheader(f"Receita por gênero - {regiao_selecionada}")
        
        receita_por_genero = df_regional.groupby('genre_name')['revenue'].sum().sort_values(ascending=False).head(10)
        
        # Converter para DataFrame para compatibilidade com Plotly
        df_genero = pd.DataFrame({
            'genre_name': receita_por_genero.index,
            'revenue': receita_por_genero.values
        })
        
        fig_genero = px.bar(df_genero, x='genre_name', y='revenue',
                            title=f'Top 10 gêneros por receita - {regiao_selecionada}')
        fig_genero.update_layout(xaxis_title="Gênero", yaxis_title="Receita (USD)")
        st.plotly_chart(fig_genero, use_container_width=True)
        
        # Análise de plataformas na região
        st.subheader(f"Receita por plataforma - {regiao_selecionada}")
        
        receita_por_plataforma = df_regional.groupby('store')['revenue'].sum().sort_values(ascending=False)
        
        fig_plataforma = px.pie(values=receita_por_plataforma.values, names=receita_por_plataforma.index,
                                title=f'Distribuição de receita por plataforma - {regiao_selecionada}')
        st.plotly_chart(fig_plataforma, use_container_width=True)
        

    else:
        st.warning(f"Nenhum dado encontrado para a região {regiao_selecionada}")

# --- ABA 3: ANÁLISES DETALHADAS ---
with tab3:
    st.header("Análises detalhadas")
    
    # Análise por gênero musical
    st.subheader("Análise por gênero musical")
    
    receita_por_genero = df_filtrado.groupby('genre_name')['revenue'].sum().sort_values(ascending=False).head(10)
    
    # Converter para DataFrame para compatibilidade com Plotly
    df_genero = pd.DataFrame({
        'genre_name': receita_por_genero.index,
        'revenue': receita_por_genero.values
    })
    
    fig_genero = px.bar(df_genero, x='genre_name', y='revenue',
                         title='Top 10 gêneros musicais por receita')
    fig_genero.update_layout(xaxis_title="Gênero Musical", yaxis_title="Receita (USD)")
    st.plotly_chart(fig_genero, use_container_width=True)
    
    # Ranking de eficiência por gênero
    st.subheader("Ranking de eficiência por gênero")
    
    # Calcular eficiência (receita/stream) por gênero
    eficiencia_por_genero = df_filtrado.groupby('genre_name').agg({
        'revenue': 'sum',
        'quantity': lambda x: x[df_filtrado.loc[x.index, 'is_stream'] == True].sum()
    }).reset_index()
    
    eficiencia_por_genero['eficiencia'] = eficiencia_por_genero['revenue'] / eficiencia_por_genero['quantity']
    eficiencia_por_genero = eficiencia_por_genero[eficiencia_por_genero['quantity'] > 0].sort_values('eficiencia', ascending=False)
    
    # Criar DataFrame para o gráfico de eficiência
    df_eficiencia = eficiencia_por_genero.head(10)[['genre_name', 'eficiencia']]
    
    fig_eficiencia = px.bar(df_eficiencia, x='genre_name', y='eficiencia',
                           title='Top 10 gêneros por eficiência (Receita/Stream)')
    fig_eficiencia.update_layout(xaxis_title="Gênero", yaxis_title="Eficiência (USD/Stream)")
    st.plotly_chart(fig_eficiencia, use_container_width=True)
    
    # Análise de sazonalidade
    st.subheader("Análise de sazonalidade")
    
    receita_por_mes = df_filtrado.groupby('mes')['revenue'].sum()
    
    # Converter para DataFrame para compatibilidade com Plotly
    df_sazonalidade = pd.DataFrame({
        'mes': receita_por_mes.index,
        'revenue': receita_por_mes.values
    })
    
    fig_sazonalidade = px.line(df_sazonalidade, x='mes', y='revenue',
                               title='Receita por mês (Sazonalidade)')
    fig_sazonalidade.update_layout(xaxis_title="Mês", yaxis_title="Receita (USD)")
    st.plotly_chart(fig_sazonalidade, use_container_width=True)

# --- ABA 4: PROJEÇÕES & ESTRATÉGIAS ---
with tab4:
    st.header("Projeções & Estratégias")
    
    # Calcular CAGR
    receita_por_ano = df_processed.groupby('ano')['revenue'].sum()
    
    if len(receita_por_ano) >= 2:
        anos = sorted(receita_por_ano.index)
        receita_inicial = receita_por_ano[anos[0]]
        receita_final = receita_por_ano[anos[-1]]
        periodo = anos[-1] - anos[0]
        
        if receita_inicial > 0 and periodo > 0:
            cagr = (receita_final / receita_inicial) ** (1/periodo) - 1
            projecao_2025 = receita_final * (1 + cagr)
            
            col1, col2, col3 = st.columns(3)
            col1.metric("CAGR Anual", f"{cagr:.1%}")
            col2.metric("Receita 2024", f"${receita_final:,.2f}")
            col3.metric("Projeção 2025", f"${projecao_2025:,.2f}")
            
            st.warning("Projeção baseada em dados limitados - use com cautela")
            
            # Gráfico de evolução
            st.subheader("Evolução anual da receita")
            
            # Converter para DataFrame para compatibilidade com Plotly
            df_evolucao = pd.DataFrame({
                'ano': receita_por_ano.index,
                'revenue': receita_por_ano.values
            })
            
            fig_evolucao = px.line(df_evolucao, x='ano', y='revenue',
                                   title='Evolução Anual da Receita')
            fig_evolucao.update_layout(xaxis_title="Ano", yaxis_title="Receita (USD)")
            st.plotly_chart(fig_evolucao, use_container_width=True)
    
    # Estratégias
    st.subheader("Estratégias para potencializar receita")
    
    # Top plataformas por eficiência
    eficiencia_por_plataforma = df_filtrado.groupby('store').agg({
        'revenue': 'sum',
        'quantity': lambda x: x[df_filtrado.loc[x.index, 'is_stream'] == True].sum()
    }).reset_index()
    
    eficiencia_por_plataforma['eficiencia'] = eficiencia_por_plataforma['revenue'] / eficiencia_por_plataforma['quantity']
    eficiencia_por_plataforma = eficiencia_por_plataforma[eficiencia_por_plataforma['quantity'] > 0].sort_values('eficiencia', ascending=False)
    
    st.write("**Top plataformas por eficiência:**")
    for idx, row in eficiencia_por_plataforma.head(5).iterrows():
        st.write(f"- {row['store']}: ${row['eficiencia']:.6f} por stream")
    
    # Recomendações
    st.subheader("Recomendações estratégicas")
    
    st.write("""
    **Baseado na análise dos dados:**
    
    - **Focar nas plataformas** com maior eficiência
    - **Expandir nos gêneros** com maior conversão
    - **Focar nos gêneros** com maior eficiência
    - **Monitorar métricas** de conversão
    - **Investir em mercados** emergentes
    """)


