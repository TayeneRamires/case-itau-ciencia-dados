# ETAPA 2 — Arquitetura Escalável e Democratização dos Dados

## Visão Geral

A solução proposta considera um cenário em que os arquivos de compras e pedidos passam a receber **vários GB de dados diariamente**, com milhares de registros para cada `data_compra`.

Nesse cenário, a solução utilizada na POC local precisa evoluir para uma arquitetura capaz de:

- armazenar grandes volumes de dados;
- processar os dados de forma escalável;
- executar cargas incrementais;
- garantir a qualidade dos dados;
- disponibilizar informações para diferentes consumidores;
- aplicar princípios de Data Mesh;
- notificar consumidores após a conclusão das cargas;
- monitorar falhas e permitir reprocessamentos.

A arquitetura proposta utiliza serviços AWS, processamento distribuído com Apache Spark e conceitos de Data Mesh.

> **Importante:** esta é uma proposta de arquitetura para um ambiente de produção. Os serviços AWS apresentados não foram implementados na POC. A implementação realizada na POC utiliza Python, PySpark e Apache Spark em ambiente local.

---

## Diagrama da Arquitetura

```text
┌─────────────────────────────────────┐
│           FONTES DE DADOS           │
│         Compras e Pedidos           │
└──────────────────┬──────────────────┘
                   │
                   ▼
┌─────────────────────────────────────┐
│          AMAZON S3 - RAW            │
│                                     │
│ • CSV original                      │
│ • Preservação dos dados recebidos   │
│ • Rastreabilidade                   │
│ • Possibilidade de reprocessamento  │
└──────────────────┬──────────────────┘
                   │
                   ▼
┌─────────────────────────────────────┐
│        AWS GLUE / PYSPARK           │
│                                     │
│ • Deduplicação                      │
│ • Validação                         │
│ • Transformações                    │
└──────────────────┬──────────────────┘
                   │
                   ▼
          ┌─────────────────┐
          │ QUALIDADE DOS   │
          │     DADOS       │
          └────────┬────────┘
                   │
          ┌────────┴────────┐
          │                 │
      APROVADO          REPROVADO
          │                 │
          ▼                 ▼
┌──────────────────┐   ┌──────────────────────┐
│ AMAZON S3        │   │ AMAZON S3           │
│ REFINED          │   │ QUARANTINE          │
│                  │   │                      │
│ • Parquet        │   │ Registros que       │
│ • Dados tratados │   │ exigem investigação │
└────────┬─────────┘   └──────────────────────┘
         │
         ▼
┌─────────────────────────────────────┐
│        AWS GLUE / PYSPARK           │
│                                     │
│ • Agregações de negócio             │
│ • Preparação para consumo           │
└──────────────────┬──────────────────┘
                   │
                   ▼
┌─────────────────────────────────────┐
│        AMAZON S3 - CURATED          │
│                                     │
│ • Parquet                           │
│ • Dados preparados para consumo     │
│ • Data Products                     │
└──────────────────┬──────────────────┘
                   │
          ┌────────┼───────────────┐
          │        │               │
          ▼        ▼               ▼
  Glue Data     Lake          EventBridge
   Catalog     Formation           │
                                Evento de
                             carga concluída
                                   │
                    ┌──────────────┼──────────────┐
                    │              │              │
                    ▼              ▼              ▼
              Conta AWS      Conta AWS      Conta AWS
              Financeiro     Marketing      Ciência de Dados
                    │              │              │
                    ▼              ▼              ▼
                   SQS            SQS            SQS


SERVIÇOS TRANSVERSAIS

Amazon MWAA / Apache Airflow
→ Orquestração
→ Dependências
→ Retries
→ Reprocessamentos

Controle de Cargas
→ Datas processadas
→ Arquivos processados
→ Status das execuções
→ Controle incremental

Amazon CloudWatch
→ Logs
→ Métricas
→ Monitoramento
→ Alarmes

Amazon SNS
→ Notificação de falhas persistentes
→ Equipe responsável
```

---

# 2.1 — Armazenamento dos Dados

Os dados seriam armazenados no **Amazon S3**, utilizado como base do Data Lake.

O S3 permite armazenar grandes volumes de dados de forma escalável e separa o armazenamento do processamento.

A estrutura proposta seria:

```text
Amazon S3
│
├── raw/
│   ├── compras/
│   └── pedidos/
│
├── refined/
│   ├── compras/
│   ├── pedidos/
│   └── validacoes/
│
├── curated/
│   ├── compras_por_cliente/
│   ├── faturamento_por_data/
│   └── vendas_por_produto/
│
└── quarantine/
    └── registros_invalidos/
```

Na camada **Raw**, os arquivos seriam mantidos inicialmente no formato original, CSV.

Nas camadas **Refined** e **Curated**, seria utilizado preferencialmente o formato **Parquet**.

O Parquet é um formato colunar, adequado para processamento analítico e leitura de grandes volumes de dados.

---

## 2.1.1 — Organização em Camadas

A arquitetura utiliza três camadas principais:

```text
RAW
 ↓
REFINED
 ↓
CURATED
```

Além delas, existe uma área auxiliar de **Quarantine**, utilizada para registros que precisam de investigação.

### Raw

A camada **Raw** contém os dados exatamente como foram recebidos da origem.

Exemplo:

```text
compras.csv
pedidos.csv
```

Mesmo que existam duplicidades ou inconsistências, os arquivos originais são preservados.

Essa camada permite:

- rastreabilidade;
- auditoria;
- recuperação dos dados originais;
- reprocessamento;
- aplicação futura de novas regras de negócio.

### Refined

A camada **Refined** contém os dados tratados e validados.

Nesta etapa seriam aplicadas regras semelhantes às utilizadas na POC:

- remoção de duplicidades;
- validação de campos obrigatórios;
- validação de tipos;
- validação de valores;
- relacionamento entre compras e pedidos;
- integridade referencial;
- reconciliação entre o valor da compra e seus pedidos;
- identificação de inconsistências.

Na POC, por exemplo, foi identificado que a compra com `id_compra = 5` possui valor registrado de **R$ 500,00**, porém não possui pedidos associados.

Essa situação precisa ser identificada antes que o registro seja considerado confiável em indicadores financeiros conciliados.

### Curated

A camada **Curated** contém os dados preparados para consumo analítico e de negócio.

Exemplos derivados da POC:

- compras por cliente;
- faturamento por data;
- vendas por produto;
- métodos de pagamento;
- indicadores derivados.

Esses conjuntos de dados podem posteriormente ser disponibilizados como **Data Products** para outros domínios.

### Quarantine

A área de **Quarantine** armazena registros que não atendam a determinados critérios de qualidade.

Exemplos:

- compras sem pedidos associados;
- divergências entre valores;
- identificadores inválidos;
- campos obrigatórios ausentes;
- problemas de estrutura;
- registros que precisam de investigação.

Esses registros podem ser analisados, corrigidos e posteriormente reprocessados.

A Quarantine não substitui Raw, Refined ou Curated. Ela funciona como uma área auxiliar para tratamento de problemas de qualidade.

---

## 2.1.2 — Formato e Particionamento dos Dados

Na camada Raw, os arquivos seriam inicialmente preservados como **CSV**, mantendo o formato recebido.

Nas camadas Refined e Curated, seria utilizado preferencialmente **Parquet**.

Os dados também poderiam ser particionados.

Para compras:

```text
/refined/compras/data_compra=2026-10-13/
```

Esse particionamento permite que o processamento consulte principalmente os dados referentes ao período necessário, evitando leituras desnecessárias de todo o histórico.

Como `pedidos.csv` não possui originalmente `data_compra`, existem algumas possibilidades.

Uma delas seria relacionar os pedidos com suas respectivas compras e utilizar a data da compra.

Outra possibilidade seria utilizar uma data de ingestão para organizar os arquivos recebidos.

O particionamento seria combinado com um mecanismo de controle das cargas já processadas.

---

# 2.2 — Processamento dos Dados

O processamento continuaria utilizando **Apache Spark**, mantendo a tecnologia utilizada na POC.

Na POC, o Spark é executado localmente.

Em produção, os jobs PySpark poderiam ser executados no **AWS Glue**, permitindo processamento distribuído em um ambiente gerenciado.

---

## 2.2.1 — Processamento Escalável com AWS Glue e PySpark

O primeiro processamento seria:

```text
S3 RAW
   ↓
AWS Glue / PySpark
   ↓
Deduplicação
Validações
Transformações
   ↓
S3 REFINED
```

Depois:

```text
S3 REFINED
   ↓
AWS Glue / PySpark
   ↓
Agregações de negócio
   ↓
S3 CURATED
```

O **AWS Glue** foi escolhido na proposta porque permite executar jobs Spark de forma gerenciada.

Isso reduz a necessidade de administrar diretamente máquinas ou clusters.

Em um cenário de crescimento do volume de dados, os recursos utilizados pelos jobs poderiam ser ajustados de acordo com a necessidade do processamento.

Outras tecnologias como Amazon EMR ou Databricks também poderiam atender cenários semelhantes, porém, nesta proposta, a arquitetura utiliza AWS Glue.

---

## 2.2.2 — Processamento Batch Incremental

O case considera uma carga de vários GB por dia.

Nesse cenário, não seria eficiente processar novamente todo o histórico sempre que uma nova carga fosse recebida.

Por isso, a proposta utiliza **processamento batch incremental diário**.

Exemplo:

```text
Já processado:

10/10
11/10
12/10

Nova carga:

13/10
```

O pipeline processaria principalmente os dados novos ou alterados referentes à nova carga.

Isso reduz:

- tempo de processamento;
- consumo computacional;
- custo;
- leitura desnecessária do histórico.

O processamento em streaming poderia ser considerado futuramente caso existisse necessidade de disponibilização em tempo real ou baixa latência.

Como o case menciona uma **carga diária**, o batch incremental é adequado para o cenário proposto.

---

## 2.2.3 — Controle de Cargas Processadas

O particionamento por data não seria o único mecanismo utilizado para controlar o processamento incremental.

O pipeline também manteria um **controle das cargas processadas**.

Esse controle poderia registrar:

- nome ou identificação do arquivo;
- data de referência;
- data e horário do processamento;
- status da execução;
- quantidade de registros;
- identificação da execução;
- versão dos dados publicados.

Exemplo:

```text
Carga:
compras_2026-10-13.csv

Data de referência:
2026-10-13

Status:
PROCESSADO

Registros:
150000

Versão:
2026-10-13
```

Esse controle permitiria identificar:

- cargas ainda não processadas;
- cargas já concluídas;
- execuções com erro;
- correções referentes a períodos anteriores;
- necessidade de reprocessamento.

Para reprocessamentos, a escrita deveria ser **idempotente**.

Isso significa que executar novamente determinada carga não deveria gerar registros duplicados ou resultados incorretos.

---

## 2.2.4 — Orquestração com Apache Airflow

O pipeline seria automatizado utilizando **Apache Airflow**, podendo ser executado na AWS por meio do **Amazon MWAA**.

O Airflow seria responsável por coordenar as dependências entre as tarefas.

Um fluxo possível seria:

```text
1. Detectar chegada da carga
        ↓
2. Consultar controle de cargas
        ↓
3. Identificar dados novos ou alterados
        ↓
4. Executar processamento PySpark
        ↓
5. Executar validações de qualidade
        ↓
6. Publicar dados aprovados na Refined
        ↓
7. Executar agregações
        ↓
8. Publicar dados na Curated
        ↓
9. Disponibilizar Data Products
        ↓
10. Atualizar controle da carga
        ↓
11. Emitir evento de carga concluída
```

Caso uma etapa necessária falhe, as tarefas dependentes não devem continuar como se a carga estivesse válida.

Isso reduz o risco de publicar dados incompletos ou inconsistentes.

---

# 2.3 — Qualidade dos Dados

A terceira exigência da Etapa 2 é definir como a qualidade dos dados seria garantida.

As validações aplicadas na POC poderiam ser ampliadas e automatizadas no pipeline de produção.

---

## 2.3.1 — Regras de Qualidade

Entre as validações possíveis:

- identificação de registros duplicados;
- campos obrigatórios nulos;
- valores inválidos;
- valores negativos quando não permitidos;
- quantidade menor ou igual a zero;
- pedido relacionado a uma compra inexistente;
- compra sem pedidos associados;
- divergência entre `valor_total` e a soma dos pedidos;
- alteração inesperada de schema;
- volume de registros muito diferente do esperado.

Fluxo:

```text
Dados processados
      ↓
Validações de qualidade
      ↓
     OK?
   /     \
 SIM     NÃO
  │       │
  ▼       ▼
Refined  Quarantine
  │
  ▼
Curated
```

---

## 2.3.2 — Critérios de Aprovação

Nem toda inconsistência precisa necessariamente interromper todo o pipeline.

A arquitetura deve possuir critérios que definam se:

- o registro pode ser tratado automaticamente;
- o registro deve ser enviado para Quarantine;
- a carga pode continuar parcialmente;
- a publicação deve ser completamente bloqueada.

Por exemplo:

- duplicidades podem ser removidas;
- registros inconsistentes podem ser identificados e segregados;
- indicadores financeiros conciliados não devem considerar registros pendentes como confirmados;
- cargas que não atinjam critérios mínimos de qualidade não devem ser publicadas como confiáveis.

Na POC, a compra de `id_compra = 5` representa um exemplo importante.

Existe:

```text
valor_total = R$ 500,00
```

Porém não existem pedidos associados à compra.

Em produção, esse registro poderia ser identificado para investigação antes de ser utilizado em um Data Product de faturamento validado.

---

## 2.3.3 — Tratamento de Registros Reprovados

Os registros reprovados seriam encaminhados para:

```text
Amazon S3
     ↓
QUARANTINE
```

A área de Quarantine permitiria:

- identificar o motivo da rejeição;
- investigar problemas;
- corrigir regras ou dados;
- manter rastreabilidade;
- reprocessar os registros posteriormente.

Os dados originais continuariam preservados na Raw.

---

# 2.4 — Utilização por Diferentes Consumidores e Data Mesh

O case também solicita uma estratégia para democratização dos dados, permitindo que diferentes equipes ou domínios consumam e contribuam com os dados de maneira eficiente e autônoma.

A proposta utiliza princípios de **Data Mesh**.

---

## 2.4.1 — Data Mesh

Data Mesh não é uma ferramenta ou serviço específico.

É uma abordagem organizacional para gestão de dados baseada principalmente em quatro princípios:

1. responsabilidade pelos dados distribuída entre domínios;
2. dados tratados como produtos;
3. plataforma de dados self-service;
4. governança federada.

Na proposta, um exemplo de domínio produtor seria:

```text
Domínio:

Vendas / Compras

Dados:

compras
pedidos
```

Esse domínio seria responsável por:

- qualidade;
- documentação;
- atualização;
- regras de negócio;
- disponibilização dos seus dados.

---

## 2.4.2 — Data Products

Os dados preparados na camada Curated poderiam ser disponibilizados como **Data Products**.

Exemplos:

```text
Compras Tratadas

Faturamento Diário

Vendas por Produto
```

Esses produtos poderiam ser consumidos por áreas ou domínios como:

```text
Financeiro

Marketing

Risco

Ciência de Dados

BI
```

O objetivo é permitir que os consumidores encontrem e utilizem os dados com mais autonomia, sem depender de processos manuais para cada nova necessidade.

---

## 2.4.3 — Catálogo e Descoberta dos Dados

O **AWS Glue Data Catalog** poderia ser utilizado para registrar os conjuntos de dados disponíveis.

Um Data Product poderia possuir metadados como:

```text
Nome:
Faturamento Diário

Domínio responsável:
Vendas

Atualização:
Diária

Formato:
Parquet

Particionamento:
data_compra

Owner:
Time responsável pelo domínio

Qualidade:
Validada
```

O catálogo facilita a descoberta dos dados.

O consumidor consegue identificar:

- quais Data Products existem;
- o que cada produto representa;
- onde está armazenado;
- qual domínio é responsável;
- frequência de atualização;
- informações de schema.

O **Amazon Athena** também poderia ser utilizado como uma opção para realizar consultas analíticas sobre os dados armazenados no S3, respeitando as permissões definidas.

---

## 2.4.4 — Governança e Controle de Acesso

A democratização dos dados não significa liberar todos os dados para todas as pessoas.

A proposta combina:

```text
AUTONOMIA
    +
GOVERNANÇA
```

O **AWS Lake Formation** poderia auxiliar na governança e no controle de acesso aos dados armazenados no Data Lake.

Os domínios poderiam possuir autonomia para manter seus Data Products, porém seguindo políticas corporativas relacionadas a:

- segurança;
- classificação dos dados;
- controle de acesso;
- qualidade;
- documentação;
- nomenclatura;
- padrões corporativos.

---

## 2.4.5 — Contratos de Dados

Os Data Products poderiam possuir contratos definidos entre produtores e consumidores.

Um contrato de dados poderia especificar:

- schema esperado;
- campos obrigatórios;
- tipos;
- frequência de atualização;
- SLA;
- domínio responsável;
- regras mínimas de qualidade.

Exemplo:

```text
Data Product:
Faturamento Diário

Atualização:
Diária

Campos obrigatórios:
data_compra
faturamento_total

Owner:
Domínio Vendas
```

Se um produtor alterasse uma característica importante do Data Product, como schema ou campos obrigatórios, essa mudança deveria ser controlada para evitar impactos nos consumidores.

---

## 2.4.6 — Notificação dos Consumidores

Depois que a carga diária fosse processada, validada e publicada na Curated, seria emitido um evento informando que o Data Product está disponível.

A proposta utiliza:

```text
Curated publicada
       ↓
Amazon EventBridge
       ↓
Contas consumidoras
       ↓
Amazon SQS
```

O **Amazon EventBridge** seria responsável por distribuir o evento.

Cada consumidor poderia possuir sua própria fila **Amazon SQS**.

Exemplo:

```text
                    Amazon EventBridge
                           │
             ┌─────────────┼─────────────┐
             │             │             │
             ▼             ▼             ▼
         Conta AWS     Conta AWS     Conta AWS
         Financeiro    Marketing     Ciência de Dados
             │             │             │
             ▼             ▼             ▼
            SQS           SQS           SQS
```

Em um cenário com múltiplas contas AWS, seriam utilizadas as configurações e permissões adequadas para comunicação entre as contas.

A proposta pode ser expandida para **N contas consumidoras**, sem que o produtor precise criar uma integração específica dentro do processamento para cada novo consumidor.

---

## 2.4.7 — Evento de Carga Concluída

Um evento poderia conter informações como:

```text
dataset: faturamento_diario

data_carga: 2026-10-13

status: SUCESSO

versao: 2026-10-13
```

O evento funciona como uma **notificação de disponibilidade**.

Os dados não precisam ser enviados dentro da mensagem.

Os dados continuam disponíveis no ambiente de dados, enquanto o evento informa ao consumidor que uma nova versão está pronta para utilização.

---

## 2.4.8 — Por que Utilizar SQS

O SQS permite desacoplar o produtor dos consumidores.

Se um consumidor estiver temporariamente indisponível, a mensagem pode permanecer na fila e ser processada posteriormente, de acordo com a configuração de retenção.

Também poderia ser utilizada uma **Dead-Letter Queue (DLQ)**.

A DLQ receberia mensagens que não foram processadas corretamente após as tentativas configuradas.

Isso ajuda a evitar perda silenciosa de notificações.

---

## 2.4.9 — Regra de Publicação

Os consumidores somente devem ser notificados após a carga ter sido processada, aprovada e publicada.

Fluxo:

```text
PROCESSAMENTO
      ↓
QUALIDADE
      ↓
APROVADO?
   /        \
 SIM        NÃO
  │          │
  ▼          ▼
CURATED   QUARANTINE
  │          │
  ▼          ▼
EVENTO     ALERTA
  │
  ▼
CONSUMIDORES
```

Portanto:

> **Os consumidores somente são notificados quando a carga considerada válida estiver disponível para consumo.**

Isso reduz o risco de outros domínios utilizarem dados incompletos ou inconsistentes acreditando que a carga diária foi concluída corretamente.

---

# 2.5 — Observabilidade

O último ponto solicitado pelo case é definir o que seria utilizado caso o pipeline apresente falhas.

A proposta utiliza principalmente:

- Amazon CloudWatch;
- Apache Airflow / Amazon MWAA;
- Amazon SNS;
- controle de cargas;
- mecanismos de reprocessamento.

---

## 2.5.1 — Monitoramento com Amazon CloudWatch

O **Amazon CloudWatch** seria utilizado para centralizar logs, métricas e alarmes.

Poderiam ser monitorados:

- falhas nos jobs Spark;
- falhas no Airflow;
- tempo de execução;
- atraso da carga;
- quantidade de registros processados;
- quantidade de registros rejeitados;
- taxa de duplicidade;
- valores nulos;
- alterações inesperadas de schema;
- falha na publicação da Curated;
- falha na publicação dos Data Products;
- falha no envio de eventos;
- comportamento das filas SQS.

---

## 2.5.2 — Retry e Alerta de Falhas

Um possível fluxo seria:

```text
Pipeline
   ↓
Erro
   ↓
Logs e métricas
   ↓
Amazon CloudWatch
   ↓
Airflow executa retry
   ↓
Continua falhando?
   ↓
SIM
   ↓
CloudWatch Alarm
   ↓
Amazon SNS
   ↓
Equipe responsável
```

O Airflow poderia executar automaticamente novas tentativas de uma tarefa.

Se a falha persistisse, o CloudWatch poderia gerar um alarme.

O alarme acionaria o **Amazon SNS**, que poderia encaminhar uma notificação para a equipe responsável.

---

## 2.5.3 — Observabilidade das Notificações

Também seria necessário acompanhar as filas SQS dos consumidores.

Poderiam ser monitoradas métricas como:

- quantidade de mensagens aguardando processamento;
- idade da mensagem mais antiga;
- falhas recorrentes no consumo;
- mensagens enviadas para DLQ.

Isso permite identificar situações em que o Data Product foi publicado corretamente, mas algum consumidor não conseguiu processar a notificação.

---

## 2.5.4 — Recuperação e Reprocessamento

Caso uma execução apresente falha, o pipeline deve permitir reprocessamento.

Como os dados originais permanecem preservados na Raw:

```text
RAW
 ↓
Falha identificada
 ↓
Correção
 ↓
Reprocessamento
 ↓
Validação
 ↓
REFINED
 ↓
CURATED
```

O controle de cargas permitiria identificar:

- qual carga falhou;
- qual arquivo foi processado;
- qual período foi afetado;
- quais etapas foram concluídas;
- o que precisa ser reprocessado;
- se uma versão já havia sido publicada.

Quando possível, seria reprocessada somente a partição ou período afetado.

A execução deveria ser idempotente para evitar duplicação de dados.

---

# 2.6 — Resumo da Arquitetura

O fluxo principal da solução pode ser resumido como:

```text
Fontes de Dados
      ↓
Amazon S3 - RAW
      ↓
AWS Glue / PySpark
      ↓
Validações de Qualidade
      │
      ├── Reprovado → Quarantine
      │
      ▼
Amazon S3 - REFINED
      ↓
AWS Glue / PySpark
      ↓
Agregações de Negócio
      ↓
Amazon S3 - CURATED
      ↓
Data Products
      ↓
Amazon EventBridge
      ↓
Contas consumidoras
      ↓
Amazon SQS
```

Serviços e conceitos transversais:

```text
Amazon MWAA / Apache Airflow
→ Orquestração, dependências, retries e reprocessamentos

Controle de Cargas
→ Processamento incremental e controle das execuções

AWS Glue Data Catalog
→ Catálogo e descoberta dos dados

AWS Lake Formation
→ Governança e controle de acesso

Amazon Athena
→ Possibilidade de consulta analítica aos dados

Amazon CloudWatch
→ Logs, métricas, monitoramento e alarmes

Amazon SNS
→ Notificação da equipe responsável em caso de falha

Amazon SQS
→ Desacoplamento entre produtor e consumidores

Data Mesh
→ Ownership por domínio, Data Products, self-service e governança federada
```

---

# 2.7 — Relação entre a POC Local e a Arquitetura de Produção

A arquitetura de produção mantém os conceitos desenvolvidos na POC, mas adapta sua execução para um cenário com volume muito maior.

| POC Local | Arquitetura de Produção |
|---|---|
| Arquivos na pasta `data/` | Amazon S3 |
| CSV original | Camada Raw |
| Python + PySpark | PySpark executado no AWS Glue |
| `dropDuplicates()` | Tratamento de duplicidades |
| Validação compras x pedidos | Regras automatizadas de qualidade |
| Identificação de inconsistências | Quarantine |
| Agregações da POC | Camada Curated |
| Execução manual | Apache Airflow / Amazon MWAA |
| Processamento completo | Batch incremental |
| Controle manual | Controle de cargas |
| Resultados no terminal | Data Products |
| Consumo local | Diferentes domínios consumidores |
| Verificação manual de problemas | CloudWatch |
| Nova execução manual | Retry e reprocessamento |
| Sem mecanismo de notificação | EventBridge + SQS |
| Investigação manual de falhas | CloudWatch + SNS |

A proposta, portanto, não substitui a lógica criada na POC.

Ela mostra **como a mesma lógica poderia evoluir para um cenário real de produção e grande volume de dados**.

---

# 2.8 — Conclusão

A arquitetura proposta atende aos cinco pontos solicitados na Etapa 2 do case.

### 1. Armazenamento dos Dados

O **Amazon S3** é utilizado como Data Lake, com organização em Raw, Refined e Curated, além de uma área de Quarantine.

Os arquivos originais são preservados em CSV e os dados tratados são armazenados preferencialmente em Parquet.

### 2. Processamento dos Dados

O processamento seria realizado com **PySpark no AWS Glue**, permitindo processamento distribuído e escalável.

A estratégia utilizada seria **batch incremental diário**, acompanhada por controle das cargas já processadas.

O **Apache Airflow / Amazon MWAA** seria responsável pela orquestração das tarefas.

### 3. Qualidade dos Dados

O pipeline aplicaria regras automáticas de qualidade para identificar duplicidades, valores inconsistentes, problemas de integridade e outras anomalias.

Registros problemáticos poderiam ser enviados para **Quarantine** para investigação.

### 4. Utilização por Diferentes Consumidores

Os dados da Curated poderiam ser disponibilizados como **Data Products**, seguindo princípios de Data Mesh.

O **AWS Glue Data Catalog** apoiaria a descoberta dos dados e o **AWS Lake Formation** apoiaria a governança e o controle de acesso.

Após uma carga válida ser publicada, o **Amazon EventBridge** poderia emitir eventos para diferentes contas consumidoras, utilizando **Amazon SQS** para desacoplar produtores e consumidores.

### 5. Observabilidade

O **Amazon CloudWatch** seria utilizado para logs, métricas, monitoramento e alarmes.

O Airflow poderia realizar retries automáticos.

Em caso de falha persistente, um alarme poderia acionar o **Amazon SNS** para notificar a equipe responsável.

Como os dados originais permanecem preservados na Raw e existe controle das cargas processadas, a arquitetura também permite reprocessar períodos afetados.

---

**A arquitetura apresentada é uma proposta conceitual para um ambiente de produção. A POC efetivamente implementada utiliza Python, PySpark e Apache Spark em ambiente local.**