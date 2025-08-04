# ONErpm Data Analyst Challenge

## sobre o projeto

Análise de dados da ONErpm para responder 4 perguntas estratégicas sobre crescimento, receita, tendências regionais e eficiência por gênero musical.

## instalação rápida

### pré-requisitos
- **Python 3.11+**
- **Poetry** (gerenciador de dependências)
- **Git**

### instalação

1. **Clone e entre no projeto:**
```bash
git clone https://github.com/gabriel-garciae/data-analyst-challenge.git
cd data-analyst-challenge
```

2. **Baixe os dados (28GB):**
```bash
# Crie a pasta e baixe os 7 arquivos .gz
mkdir -p data/raw
# Links abaixo
```

[onerpm_data_analyst_test_data_2022_csv000.gz](https://1r-test-statsload.s3.dualstack.us-east-1.amazonaws.com/onerpm_data_analyst_test_data/onerpm_data_analyst_test_data_2022_csv000.gz?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIA3YUKOX75BLGJJRWV/20250731/us-east-1/s3/aws4_request&X-Amz-Date=20250731T202554Z&X-Amz-Expires=604798&X-Amz-SignedHeaders=host&X-Amz-Signature=9b6b2f24273b62bdf3e52cbd11baaf4133c4786f7af33884a66272ec967fd3e4)

[onerpm_data_analyst_test_data_2022_csv001.gz](https://1r-test-statsload.s3.dualstack.us-east-1.amazonaws.com/onerpm_data_analyst_test_data/onerpm_data_analyst_test_data_2022_csv001.gz?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIA3YUKOX75BLGJJRWV/20250731/us-east-1/s3/aws4_request&X-Amz-Date=20250731T202611Z&X-Amz-Expires=604797&X-Amz-SignedHeaders=host&X-Amz-Signature=f9ce822b3e3d712d8028faa23f9a9d1e38bb45d3a43978df0ae2df1f098624f1)

[onerpm_data_analyst_test_data_2023_csv000.gz](https://1r-test-statsload.s3.dualstack.us-east-1.amazonaws.com/onerpm_data_analyst_test_data/onerpm_data_analyst_test_data_2023_csv000.gz?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIA3YUKOX75BLGJJRWV/20250731/us-east-1/s3/aws4_request&X-Amz-Date=20250731T202624Z&X-Amz-Expires=604798&X-Amz-SignedHeaders=host&X-Amz-Signature=db9482f1d06e5344c5ba5f64345eb8527f94a72494afb3a2416d581f8249cd96)

[onerpm_data_analyst_test_data_2023_csv001.gz](https://1r-test-statsload.s3.dualstack.us-east-1.amazonaws.com/onerpm_data_analyst_test_data/onerpm_data_analyst_test_data_2023_csv001.gz?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIA3YUKOX75BLGJJRWV/20250731/us-east-1/s3/aws4_request&X-Amz-Date=20250731T202641Z&X-Amz-Expires=604798&X-Amz-SignedHeaders=host&X-Amz-Signature=e04c276a359c4633892bfdc64f1f7d7c78acdd1c86cb2e45fe2e8acac8e233b6)

[onerpm_data_analyst_test_data_2024_csv000.gz](https://1r-test-statsload.s3.dualstack.us-east-1.amazonaws.com/onerpm_data_analyst_test_data/onerpm_data_analyst_test_data_2024_csv000.gz?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIA3YUKOX75BLGJJRWV/20250731/us-east-1/s3/aws4_request&X-Amz-Date=20250731T202703Z&X-Amz-Expires=604799&X-Amz-SignedHeaders=host&X-Amz-Signature=d64ffd4d22e0fa8160105936b08222209ca8328721d78b41f1ec6285e5369cd7)

[onerpm_data_analyst_test_data_2024_csv001.gz](https://1r-test-statsload.s3.dualstack.us-east-1.amazonaws.com/onerpm_data_analyst_test_data/onerpm_data_analyst_test_data_2024_csv001.gz?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIA3YUKOX75BLGJJRWV/20250731/us-east-1/s3/aws4_request&X-Amz-Date=20250731T202713Z&X-Amz-Expires=604797&X-Amz-SignedHeaders=host&X-Amz-Signature=773fcf27ead30802b96ccf4011651afa6915c10ee4f2eb2d37a4c3f43fd1eaa4)

[onerpm_data_analyst_test_data_2024_csv002.gz](https://1r-test-statsload.s3.dualstack.us-east-1.amazonaws.com/onerpm_data_analyst_test_data/onerpm_data_analyst_test_data_2024_csv002.gz?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIA3YUKOX75BLGJJRWV/20250731/us-east-1/s3/aws4_request&X-Amz-Date=20250731T202720Z&X-Amz-Expires=604798&X-Amz-SignedHeaders=host&X-Amz-Signature=9389e6879e3c350f175793ca67f02050cacd3a4d7b0ccc9c25b3c7ecd60eed76)

3. **Instale as dependências:**
```bash
# Instalar Poetry (se não tiver)
curl -sSL https://install.python-poetry.org | python3 -

# Instalar dependências do projeto
poetry install
```

4. **Configure e execute dbt:**
```bash
# Instalar dependências dbt
poetry run dbt deps

# Executar modelos para criar as análises
poetry run dbt run
```

5. **Execute o dashboard:**
```bash
# Navegar para pasta do dashboard
cd onerpm

# Executar dashboard Streamlit
poetry run streamlit run app.py
# Acesse: http://localhost:8501
```

## respostas às 4 perguntas

### **1. Projeção de crescimento 2025**
- **CAGR**: 11.6% (Crescimento Alto)
- **Projeção 2025**: $80.6M
- **Insight**: Crescimento sustentável

### **2. Melhor estratégia para receita**
- **Plataforma**: YouTube Shorts
- **Eficiência**: 0.114 (Alta Eficiência)
- **Por que focar**: Maior retorno por stream (receita/stream = 0.114)
- **Comparação**: Outras plataformas têm eficiência menor
- **Recomendação**: Focar investimentos nesta plataforma

### **3. Tendências regionais**
- **BV**: 7,959% crescimento (Alto Crescimento)
- **AZ**: 1,396% crescimento (Alto Crescimento)
- **PG**: 911% crescimento (Alto Crescimento)
- **Insight**: Mercados emergentes com alto potencial

### **4. Melhor gênero (conversão streams x receita)**
- **Gênero**: Enka
- **Eficiência**: 0.003465 (Média Eficiência)
- **Insight**: Gênero japonês com boa conversão

## como executar

### **Dashboard Interativo**
```bash
cd onerpm
poetry run streamlit run app.py
# Acesse: http://localhost:8501
```

### **Queries Básicas**
```bash
poetry run duckdb onerpm.duckdb < queries_basicas_readme.sql
```

### **Modelos dbt**
```bash
poetry run dbt run
```

## estrutura do projeto

```
data-analyst-challenge/
├── onerpm/app.py              # Dashboard Streamlit
├── models/marts/             # Modelos dbt (4 modelos)
├── queries_basicas_readme.sql # Queries das 4 perguntas
├── scripts/load_to_duckdb.py  # Script de ingestão
├── data/raw/                  # Dados brutos (.gz)
├── dbt_project.yml           # Configuração dbt
├── pyproject.toml            # Dependências Python
└── README.md                 # Este arquivo
```

## stack tecnológico

- **ETL**: dbt + DuckDB
- **Dashboard**: Streamlit
- **Dados**: 28GB brutos (2022-2024)
- **Análise**: 10M registros para performance

## requisitos de sistema

- **RAM**: 8GB mínimo (16GB recomendado)
- **Armazenamento**: 100GB livres
- **CPU**: 4 cores mínimo
- **Tempo**: 5-15 min ingestão, 2-5 min dbt
## solução de problemas

### **Poetry não encontrado**
```bash
curl -sSL https://install.python-poetry.org | python3 -
```

### **Dados não carregados**
```bash
ls -la data/raw/*.gz  # Verificar se há 7 arquivos
python scripts/load_to_duckdb.py  # Reexecutar
```

### **dbt não funciona**
```bash
poetry install
poetry run dbt deps
```

## principais insights

### **ações recomendadas**
1. **Focar em YouTube Shorts** - Maior eficiência de receita por stream
2. **Expandir em mercados emergentes** - BV, AZ, PG
3. **Desenvolver catálogo Enka** - Boa conversão

### **crescimento**
- **CAGR 11.6%** indica crescimento sustentável
- **Projeção $80.6M** para 2025
- **Foco em eficiência** por plataforma
