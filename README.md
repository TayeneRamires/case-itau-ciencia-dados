# Case Técnico — Ciência de Dados

Case desenvolvido como parte de um processo seletivo para estágio em Ciência de Dados.

O projeto possui duas etapas:

1. processamento e análise dos dados utilizando Apache Spark com PySpark;
2. proposta de uma arquitetura escalável para um ambiente de produção, considerando grandes volumes de dados, qualidade, observabilidade e princípios de Data Mesh.

---

## Objetivo

A primeira etapa tem como objetivo processar dois arquivos CSV contendo informações de compras e pedidos de uma loja online.

O processamento foi desenvolvido com **Python, PySpark e Apache Spark** e contempla:

- identificação e remoção de dados duplicados;
- validação entre o valor registrado da compra e a soma dos pedidos associados;
- identificação de inconsistências;
- total de compras realizadas por cliente;
- identificação do método de pagamento mais utilizado;
- identificação dos produtos mais vendidos;
- identificação das datas com maior faturamento;
- apresentação dos resultados diretamente no terminal.

A segunda etapa apresenta uma proposta de arquitetura para um cenário em que os arquivos passam a receber vários GB de dados diariamente.

A arquitetura considera:

- armazenamento escalável;
- organização dos dados em camadas Raw, Refined e Curated;
- processamento distribuído;
- processamento batch incremental;
- orquestração;
- qualidade dos dados;
- Data Mesh e Data Products;
- utilização dos dados por diferentes consumidores;
- notificação após a conclusão das cargas;
- observabilidade;
- tratamento de falhas e reprocessamento.

---

## Etapa 1 — Processamento com Spark

A implementação da primeira etapa foi desenvolvida localmente utilizando **PySpark**.

### Deduplicação dos Dados

Os arquivos `compras.csv` e `pedidos.csv` possuem registros duplicados.

O processamento identifica as duplicidades e remove registros repetidos antes das análises.

Resultado:

```text
compras.csv
7 registros originais
5 registros após deduplicação
2 duplicados removidos

pedidos.csv
7 registros originais
5 registros após deduplicação
2 duplicados removidos
```

---

### Validação entre Compras e Pedidos

Para cada compra, é calculado o valor total dos pedidos associados:

```text
quantidade × preco_unitario
```

Depois, esse valor é comparado com o campo `valor_total` existente em `compras.csv`.

A validação permite classificar as compras e identificar inconsistências.

Um dos casos encontrados na base foi:

```text
id_compra = 5
valor_total = R$ 500,00
pedidos associados = nenhum
```

A compra permanece visível na análise para que a ausência de pedidos possa ser identificada.

---

### Análise de Compras por Cliente

O processamento calcula a quantidade de compras realizadas por cada cliente.

Resultado:

| cliente_id | quantidade de compras |
|---|---:|
| 101 | 2 |
| 102 | 1 |
| 103 | 1 |
| 104 | 1 |

---

### Método de Pagamento Mais Utilizado

O método de pagamento com maior número de compras foi:

```text
Cartão de Crédito
3 compras
```

---

### Produtos Mais Vendidos

Os produtos são classificados de acordo com a soma das quantidades vendidas.

Resultado:

| produto_id | quantidade |
|---|---:|
| A003 | 4 |
| A004 | 3 |
| A001 | 2 |
| A002 | 1 |
| A005 | 1 |

O produto com maior quantidade vendida foi:

```text
A003
4 unidades
```

---

### Datas com Maior Faturamento

O faturamento registrado é agrupado por `data_compra`.

Resultado:

| data | faturamento registrado |
|---|---:|
| 2023-10-05 | R$ 500,00 |
| 2023-10-03 | R$ 300,75 |
| 2023-10-02 | R$ 200,00 |
| 2023-10-01 | R$ 150,50 |
| 2023-10-04 | R$ 50,00 |

A maior data de faturamento registrado foi:

```text
2023-10-05
R$ 500,00
```

Esse valor pertence à compra de `id_compra = 5`, que não possui pedidos associados.

Por isso, o projeto diferencia **faturamento registrado** de um valor financeiro considerado conciliado.

---

## Etapa 2 — Arquitetura Escalável e Democratização dos Dados

A segunda etapa propõe uma arquitetura para um cenário em que os arquivos de compras e pedidos passam a receber vários GB de dados diariamente.

A arquitetura foi organizada de acordo com os cinco pontos solicitados no case:

### 2.1 — Armazenamento dos Dados

- Amazon S3 como base do Data Lake;
- CSV preservado na camada Raw;
- Parquet nas camadas tratadas;
- organização em Raw, Refined e Curated;
- Quarantine para registros que necessitem de investigação;
- particionamento dos dados para otimizar leituras.

### 2.2 — Processamento dos Dados

- Apache Spark com PySpark;
- AWS Glue como ambiente gerenciado para execução dos jobs;
- processamento distribuído;
- processamento batch incremental diário;
- controle das cargas já processadas;
- Apache Airflow / Amazon MWAA para orquestração;
- retries e reprocessamento controlado.

### 2.3 — Qualidade dos Dados

Entre as validações propostas:

- duplicidades;
- campos obrigatórios;
- tipos de dados;
- valores inválidos;
- relacionamento entre compras e pedidos;
- integridade referencial;
- reconciliação financeira;
- alterações inesperadas de schema;
- volumes inesperados de registros.

Dados que não atendam aos critérios definidos podem ser direcionados para uma área de **Quarantine** para investigação e posterior reprocessamento.

### 2.4 — Utilização por Diferentes Consumidores e Data Mesh

A proposta utiliza princípios de Data Mesh:

- responsabilidade pelos dados distribuída entre domínios;
- dados tratados como produtos;
- plataforma de dados self-service;
- governança federada.

Exemplos de Data Products:

- Compras Tratadas;
- Faturamento Diário;
- Vendas por Produto.

Esses produtos podem ser utilizados por diferentes áreas ou domínios, como:

- Financeiro;
- Marketing;
- Risco;
- Ciência de Dados;
- BI.

Após a publicação de uma carga válida, o **Amazon EventBridge** pode emitir um evento de disponibilidade.

Cada consumidor pode possuir sua própria fila **Amazon SQS**, inclusive em diferentes contas AWS.

Fluxo simplificado:

```text
Carga processada
      ↓
Qualidade aprovada
      ↓
S3 Curated
      ↓
Data Product disponível
      ↓
Amazon EventBridge
      ↓
Amazon SQS
      ↓
Diferentes consumidores
```

### 2.5 — Observabilidade

A arquitetura utiliza o **Amazon CloudWatch** para centralização de:

- logs;
- métricas;
- monitoramento;
- alarmes.

Em caso de falha:

```text
Pipeline
   ↓
Erro
   ↓
CloudWatch
   ↓
Retry pelo Airflow
   ↓
Falha persistente
   ↓
CloudWatch Alarm
   ↓
Amazon SNS
   ↓
Equipe responsável
```

Como os dados originais permanecem preservados na camada Raw, uma carga com problema pode ser corrigida e reprocessada.

---

## Arquitetura Proposta

Fluxo principal:

```text
Fontes de Dados
      ↓
Amazon S3 - RAW
      ↓
AWS Glue / PySpark
      ↓
Qualidade dos Dados
      │
      ├── Reprovado → Quarantine
      │
      ▼
Amazon S3 - REFINED
      ↓
AWS Glue / PySpark
      ↓
Amazon S3 - CURATED
      ↓
Data Products
      ↓
Amazon EventBridge
      ↓
Amazon SQS
      ↓
Diferentes consumidores
```

Serviços e conceitos complementares:

```text
Amazon MWAA / Apache Airflow
→ Orquestração do pipeline

AWS Glue Data Catalog
→ Catálogo e descoberta dos dados

AWS Lake Formation
→ Governança e controle de acesso

Amazon Athena
→ Consultas analíticas

Amazon CloudWatch
→ Logs, métricas e alarmes

Amazon SNS
→ Notificação de falhas

Controle de Cargas
→ Processamento incremental e reprocessamento

Data Mesh
→ Ownership por domínio, Data Products, self-service e governança federada
```

> **Importante:** a arquitetura AWS representa uma proposta conceitual para produção. A POC efetivamente implementada utiliza Python, PySpark e Apache Spark em ambiente local.

---

## Tecnologias

### Utilizadas na POC

- Python
- PySpark
- Apache Spark
- Git
- GitHub

### Propostas para o Ambiente de Produção

- Amazon S3
- AWS Glue
- Apache Spark / PySpark
- Apache Airflow
- Amazon MWAA
- AWS Glue Data Catalog
- AWS Lake Formation
- Amazon Athena
- Amazon EventBridge
- Amazon SQS
- Amazon CloudWatch
- Amazon SNS

---

## Estrutura do Projeto

```text
case-itau-ciencia-dados/
│
├── data/
│   ├── compras.csv
│   └── pedidos.csv
│
├── docs/
│   └── documentação da arquitetura
│
├── output/
│   └── saídas geradas pelo processamento
│
├── src/
│   └── código PySpark
│
├── README.md
├── requirements.txt
└── .gitignore
```

---

## Execução

Instale as dependências do projeto:

```bash
pip install -r requirements.txt
```

Execute o processamento:

```bash
python src/processamento.py
```

Os resultados das análises são exibidos diretamente no terminal, conforme solicitado no case.

---

## Relação entre a POC e a Proposta de Produção

| POC Local | Arquitetura de Produção |
|---|---|
| Arquivos locais | Amazon S3 |
| CSV original | S3 Raw |
| Python + PySpark | PySpark no AWS Glue |
| Deduplicação | Tratamento da camada Refined |
| Validação compras x pedidos | Regras automatizadas de qualidade |
| Inconsistências identificadas | Quarantine |
| Agregações | S3 Curated |
| Execução manual | Airflow / Amazon MWAA |
| Processamento completo | Batch incremental |
| Resultados no terminal | Data Products |
| Verificação manual | CloudWatch |
| Nova execução manual | Retry e reprocessamento |
| Sem notificação automática | EventBridge + SQS |

---

## Conclusão

A POC demonstra o processamento dos dados de compras e pedidos utilizando Apache Spark com PySpark.

A solução trata duplicidades, realiza validações entre as bases e gera os indicadores solicitados pelo case.

A arquitetura proposta mostra como essa mesma lógica poderia evoluir para um ambiente de produção com vários GB de dados diariamente, adicionando escalabilidade, processamento incremental, qualidade, democratização dos dados, Data Mesh, notificações entre consumidores e observabilidade.

Dessa forma, a implementação local e a arquitetura proposta representam duas etapas da mesma solução: primeiro o processamento funcional dos dados e, posteriormente, sua evolução para um cenário de produção escalável.