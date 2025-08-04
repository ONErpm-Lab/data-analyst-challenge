# Guia de utilização - ONErpm data analyst challenge

## Pré-requisitos

### Requisitos de hardware
- **RAM**: 8GB mínimo (16GB recomendado)
- **Armazenamento**: 100GB livres
- **CPU**: 4 cores mínimo
- **Conexão**: Internet estável para download dos dados

### Software Necessário
- **Python**: 3.11 ou superior
- **Git**: Para clonar o repositório
- **Poetry**: Gerenciador de dependências (será instalado automaticamente)

## Passo a passo completo

### 1. Preparação do ambiente

#### Instalar Python
```bash
# macOS (com Homebrew)
brew install python@3.11

# Ubuntu/Debian
sudo apt update
sudo apt install python3.11 python3.11-pip

# Windows
# Baixar de https://www.python.org/downloads/
```

#### Verificar instalação
```bash
python3 --version
# Deve mostrar: Python 3.11.x
```

### 2. Clonar o repositório

```bash
# Clone o repositório
git clone https://github.com/gabriel-garciae/data-analyst-challenge.git
cd data-analyst-challenge

# Verificar estrutura
ls -la
```

### 3. Download dos dados

#### Criar diretório
```bash
mkdir -p data/raw
```

#### Download manual (recomendado)
Baixe os 7 arquivos abaixo e coloque na pasta `data/raw/`:

[onerpm_data_analyst_test_data_2022_csv000.gz](https://1r-test-statsload.s3.dualstack.us-east-1.amazonaws.com/onerpm_data_analyst_test_data/onerpm_data_analyst_test_data_2022_csv000.gz?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIA3YUKOX75BLGJJRWV/20250731/us-east-1/s3/aws4_request&X-Amz-Date=20250731T202554Z&X-Amz-Expires=604798&X-Amz-SignedHeaders=host&X-Amz-Signature=9b6b2f24273b62bdf3e52cbd11baaf4133c4786f7af33884a66272ec967fd3e4)

[onerpm_data_analyst_test_data_2022_csv001.gz](https://1r-test-statsload.s3.dualstack.us-east-1.amazonaws.com/onerpm_data_analyst_test_data/onerpm_data_analyst_test_data_2022_csv001.gz?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIA3YUKOX75BLGJJRWV/20250731/us-east-1/s3/aws4_request&X-Amz-Date=20250731T202611Z&X-Amz-Expires=604797&X-Amz-SignedHeaders=host&X-Amz-Signature=f9ce822b3e3d712d8028faa23f9a9d1e38bb45d3a43978df0ae2df1f098624f1)

[onerpm_data_analyst_test_data_2023_csv000.gz](https://1r-test-statsload.s3.dualstack.us-east-1.amazonaws.com/onerpm_data_analyst_test_data/onerpm_data_analyst_test_data_2023_csv000.gz?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIA3YUKOX75BLGJJRWV/20250731/us-east-1/s3/aws4_request&X-Amz-Date=20250731T202624Z&X-Amz-Expires=604798&X-Amz-SignedHeaders=host&X-Amz-Signature=db9482f1d06e5344c5ba5f64345eb8527f94a72494afb3a2416d581f8249cd96)

[onerpm_data_analyst_test_data_2023_csv001.gz](https://1r-test-statsload.s3.dualstack.us-east-1.amazonaws.com/onerpm_data_analyst_test_data/onerpm_data_analyst_test_data_2023_csv001.gz?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIA3YUKOX75BLGJJRWV/20250731/us-east-1/s3/aws4_request&X-Amz-Date=20250731T202641Z&X-Amz-Expires=604798&X-Amz-SignedHeaders=host&X-Amz-Signature=e04c276a359c4633892bfdc64f1f7d7c78acdd1c86cb2e45fe2e8acac8e233b6)

[onerpm_data_analyst_test_data_2024_csv000.gz](https://1r-test-statsload.s3.dualstack.us-east-1.amazonaws.com/onerpm_data_analyst_test_data/onerpm_data_analyst_test_data_2024_csv000.gz?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIA3YUKOX75BLGJJRWV/20250731/us-east-1/s3/aws4_request&X-Amz-Date=20250731T202703Z&X-Amz-Expires=604799&X-Amz-SignedHeaders=host&X-Amz-Signature=d64ffd4d22e0fa8160105936b08222209ca8328721d78b41f1ec6285e5369cd7)

[onerpm_data_analyst_test_data_2024_csv001.gz](https://1r-test-statsload.s3.dualstack.us-east-1.amazonaws.com/onerpm_data_analyst_test_data/onerpm_data_analyst_test_data_2024_csv001.gz?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIA3YUKOX75BLGJJRWV/20250731/us-east-1/s3/aws4_request&X-Amz-Date=20250731T202713Z&X-Amz-Expires=604797&X-Amz-SignedHeaders=host&X-Amz-Signature=773fcf27ead30802b96ccf4011651afa6915c10ee4f2eb2d37a4c3f43fd1eaa4)

[onerpm_data_analyst_test_data_2024_csv002.gz](https://1r-test-statsload.s3.dualstack.us-east-1.amazonaws.com/onerpm_data_analyst_test_data/onerpm_data_analyst_test_data_2024_csv002.gz?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIA3YUKOX75BLGJJRWV/20250731/us-east-1/s3/aws4_request&X-Amz-Date=20250731T202720Z&X-Amz-Expires=604798&X-Amz-SignedHeaders=host&X-Amz-Signature=9389e6879e3c350f175793ca67f02050cacd3a4d7b0ccc9c25b3c7ecd60eed76)

#### Verificar Download
```bash
ls -la data/raw/*.gz
# Deve mostrar 7 arquivos com tamanhos entre 1GB e 5GB (estão compactados)
```

### 4. Instalação das dependências

#### Instalar Poetry
```bash
# Instalação automática
curl -sSL https://install.python-poetry.org | python3 -

# Verificar instalação
poetry --version
```

#### Instalar eependências do projeto
```bash
# Instalar dependências
poetry install

# Verificar instalação
poetry show
```

### 5. Configuração do dbt

#### Criar Configuração
```bash
# Criar diretório .dbt
mkdir -p ~/.dbt

# Criar arquivo profiles.yml
cat > ~/.dbt/profiles.yml << EOF
data_analyst_challenge:
  outputs:
    dev:
      type: duckdb
      path: $(pwd)/onerpm.duckdb
      threads: 4
  target: dev
EOF
```

#### Instalar dependências dbt
```bash
poetry run dbt deps
```

### 6. Processamento dos dados

#### Ingestão dos dados
```bash
# Executar script de ingestão
poetry run python scripts/load_to_duckdb.py

# Aguardar conclusão (5-15 minutos)
```

#### Executar modelos dbt
```bash
# Executar todos os modelos
poetry run dbt run

# Aguardar conclusão (2-5 minutos)
```

### 7. Executar dashboard

#### Iniciar Dashboard
```bash
# Navegar para pasta do dashboard
cd onerpm

# Executar dashboard
poetry run streamlit run app.py
```

#### Acessar dashboard
- **URL**: http://localhost:8501

## Utilização do dashboard

### Visão executiva
- **KPIs Principais**: Receita total, streams, downloads, eficiência
- **Gráficos**: Evolução da receita, top países
- **Filtros**: Por período, gênero, país

### Visão regional
- **Seleção de Região**: Escolha um país específico
- **Métricas Regionais**: Receita, streams, downloads da região
- **Análises**: Gêneros e plataformas por região

### Análises detalhadas
- **Produtos**: Top produtos por receita
- **Eficiência por Gênero**: Ranking de conversão
- **Sazonalidade**: Padrões mensais

### Projeções & estratégias
- **CAGR**: Taxa de crescimento anual
- **Projeção 2025**: Estimativa baseada em dados históricos
- **Estratégias**: Recomendações baseadas em análise

### Respostas README
- **4 Perguntas**: Respostas diretas às questões do desafio
- **Métricas**: Valores específicos e classificações

## Comandos uteis

### Verificar Status
```bash
# Verificar dados carregados
poetry run duckdb onerpm.duckdb -c "SELECT COUNT(*) FROM stg_onerpm_raw;"

# Verificar modelos dbt
poetry run dbt ls
```

### Executar queries específicas
```bash
# Queries básicas do README
poetry run duckdb onerpm.duckdb < queries_basicas_readme.sql

# Query personalizada
poetry run duckdb onerpm.duckdb -c "SELECT * FROM projecao_2025;"
```

### Manutenção
```bash
# Limpar cache dbt
poetry run dbt clean

# Atualizar dependências
poetry update

# Verificar logs
tail -f logs/dbt.log
```

## Solução de problemas

### Erro: Poetry não encontrado
```bash
# Reinstalar Poetry
curl -sSL https://install.python-poetry.org | python3 -
export PATH="$HOME/.local/bin:$PATH"
```

### Erro: dados não carregados
```bash
# Verificar arquivos
ls -la data/raw/*.gz

# Reexecutar ingestão
poetry run python scripts/load_to_duckdb.py
```

### Erro: dbt não funciona
```bash
# Reinstalar dependências
poetry install
poetry run dbt deps
poetry run dbt run
```

### Erro: dashboard não carrega
```bash
# Verificar porta
lsof -i :8501

# Reiniciar dashboard
cd onerpm
poetry run streamlit run app.py
```

### Erro: Memória insuficiente
```bash
# Reduzir amostra no dashboard
# Editar onerpm/app.py linha 15
# Alterar LIMIT 10000000 para LIMIT 5000000
```

## Informações técnicas

### Estrutura dos dados
- **store**: Plataforma (Amazon, YouTube, Spotify, Facebook)
- **date**: Data da transação
- **product**: Produto
- **quantity**: Quantidade
- **is_stream**: Se é stream (TRUE/FALSE)
- **is_download**: Se é download (TRUE/FALSE)
- **revenue**: Receita
- **currency**: Moeda
- **country_code**: Código do país
- **genre_id**: ID do gênero
- **genre_name**: Nome do gênero

### Modelos dbt
- **stg_onerpm_raw**: Dados limpos
- **projecao_2025**: CAGR e projeção
- **estrategia_receita**: Eficiência por plataforma/gênero
- **tendencias_regionais**: Crescimento por país
- **analytics_dashboard**: Dashboard consolidado

### Performance
- **Ingestão**: 5-15 minutos
- **dbt run**: 2-5 minutos
- **Dashboard**: Carregamento instantâneo
- **Amostra**: 10M registros (limitação de memória)

## Observações importantes

### Limitações do smbiente Local
- **Modelos como view**: Devido ao espaço limitado em disco
- **Amostra de dados**: 10M registros para evitar estouro de memória
- **Performance**: Pode variar conforme hardware

### Ambiente corporativo
Em ambiente corporativo com máquinas mais robustas:
- Modelos podem ser materializados como tabelas
- Dashboard pode usar dados completos
- Performance será significativamente melhor