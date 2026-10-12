
# Bibliotecas utilizadas na configuração do ambiente Python
import os
import sys

# Gerenciamento da sessão Spark
from pyspark.sql import SparkSession

# Tipos de dados utilizados para definir os schemas dos arquivos CSV
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DateType,
    DecimalType
)

# Configura o PySpark para utilizar o interpretador Python da execução atual,
# mantendo a consistência entre o processo principal (driver) e os workers.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


# ============================================================
# 1. INICIALIZAÇÃO DO SPARK
# ============================================================

# Inicializa a sessão Spark em modo local, utilizando os núcleos
# de processamento disponíveis na máquina.
spark = (
    SparkSession.builder
    .appName("CaseCienciaDados")
    .master("local[*]")
    .getOrCreate()
)

# Define o nível de log para destacar avisos e erros,
# reduzindo mensagens informativas durante a execução.
spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 2. DEFINIÇÃO DOS SCHEMAS
# ============================================================

# Define a estrutura e os tipos de dados do arquivo de compras,
# garantindo a interpretação adequada de identificadores, datas
# e valores monetários.
schema_compras = StructType([
    StructField("id_compra", IntegerType(), True),
    StructField("cliente_id", IntegerType(), True),
    StructField("data_compra", DateType(), True),
    StructField("valor_total", DecimalType(10, 2), True),
    StructField("metodo_pagamento", StringType(), True)
])


# Define a estrutura do arquivo de pedidos, especificando os tipos
# dos identificadores, quantidades e preços unitários dos produtos.
schema_pedidos = StructType([
    StructField("id_pedido", IntegerType(), True),
    StructField("id_compra", IntegerType(), True),
    StructField("produto_id", StringType(), True),
    StructField("quantidade", IntegerType(), True),
    StructField("preco_unitario", DecimalType(10, 2), True)
])


# ============================================================
# 3. LEITURA DOS ARQUIVOS
# ============================================================

# Carrega o arquivo de compras utilizando o schema definido,
# considerando a primeira linha como cabeçalho e preservando
# a codificação e o formato das datas.
df_compras = (
    spark.read
    .option("header", True)
    .option("encoding", "UTF-8")
    .option("dateFormat", "yyyy-MM-dd")
    .schema(schema_compras)
    .csv("data/compras.csv")
)


# Carrega o arquivo de pedidos com o schema previamente definido,
# garantindo a consistência dos tipos de dados utilizados nas análises.
df_pedidos = (
    spark.read
    .option("header", True)
    .option("encoding", "UTF-8")
    .schema(schema_pedidos)
    .csv("data/pedidos.csv")
)


# ============================================================
# 4. VALIDAÇÃO DA LEITURA
# ============================================================

# Exibe os registros originais de compras para conferência
# antes da aplicação das regras de tratamento dos dados.
print("\n" + "=" * 70)
print("COMPRAS — DADOS ORIGINAIS")
print("=" * 70)

df_compras.show(truncate=False)


# Exibe os registros originais de pedidos, permitindo verificar
# os dados carregados e identificar possíveis duplicações.
print("\n" + "=" * 70)
print("PEDIDOS — DADOS ORIGINAIS")
print("=" * 70)

df_pedidos.show(truncate=False)


# Apresenta a estrutura do DataFrame de compras para verificar
# os nomes das colunas e os tipos de dados atribuídos.
print("\n" + "=" * 70)
print("SCHEMA — COMPRAS")
print("=" * 70)

df_compras.printSchema()


# Apresenta a estrutura do DataFrame de pedidos para confirmar
# os tipos de dados definidos no carregamento.
print("\n" + "=" * 70)
print("SCHEMA — PEDIDOS")
print("=" * 70)

df_pedidos.printSchema()


# Contabiliza os registros carregados em cada DataFrame,
# estabelecendo uma referência para a etapa de deduplicação.
qtd_compras = df_compras.count()
qtd_pedidos = df_pedidos.count()


# Exibe as quantidades de registros originais carregados,
# incluindo as possíveis duplicações presentes nos arquivos.
print("\n" + "=" * 70)
print("QUANTIDADE DE REGISTROS CARREGADOS")
print("=" * 70)

print(f"Compras: {qtd_compras}")
print(f"Pedidos: {qtd_pedidos}")


# ============================================================
# 5. ENCERRAMENTO
# ============================================================

# Encerra a sessão Spark e libera os recursos associados à execução.
spark.stop()
