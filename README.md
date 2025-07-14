
## Planejamento de Projeto

 Dado o desafio e os arquivos de dados disponibilizados , planejei e desenvolvi um script (ingest.py) para ingestão dos arquivos integrando com o BigQuery. Após analisado a volumetria foi preciso criar chunks de envio para criação da tabela tb_dados_one_rpm. 



### Using the onerpm-dbt

Antes de executar verifique as configs no profiles.yaml ( Ex: C:\Users\.dbt) e configure como método de auth 'service account' e apontando para o caminho onde está o json de credentials do GCP : onerpm-dbt-14a34acb17b7.json


Para rodar use os seguintes comandos:
- dbt run
- dbt test



## Importando o painel `.pbix`

1. Abra o arquivo [`onerpm_dashboard.pbix`](https://drive.google.com/file/d/1s41FAt5QrvNeeFOrW8Qi4il6q-1KMiqJ/view?usp=sharing) com o Power BI Desktop
2. Caso peça autenticação:
   - Use a conta de serviço : dbt-user@onerpm-dbt.iam.gserviceaccount.com com permissão no projeto GCP `onerpm-dbt` com o conteúdo [json](https://drive.google.com/file/d/121a6oCi6M4siwQlFPOoPalLxEgWbZHAl/view?usp=sharing) de autenticação
3. Visualize os gráficos e análises feitas.
4. Para atualizar os dados, clique em `Atualizar` (se o painel estiver conectado ao BigQuery)


## Gráficos disponíveis:


* Receita total e conversão por stream
* Revenue por ano e country_code
* Projeção 2025
* Receita total por store




