# Bibliotecas utilizadas na configuração do ambiente Python
import os
import sys

# Gerenciamento da sessão Spark e funções utilizadas
# nas transformações e validações dos DataFrames.
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Tipos de dados utilizados para definir os schemas dos arquivos CSV.
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DateType,
    DecimalType
)

# Configura o PySpark para utilizar o mesmo interpretador Python
# utilizado na execução atual, mantendo a consistência entre
# o processo principal (driver) e os workers.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


# ============================================================
# 1. INICIALIZAÇÃO DO SPARK
# ============================================================

# Inicializa a sessão Spark em modo local.
# O parâmetro local[*] permite utilizar os núcleos de processamento
# disponíveis na máquina durante a execução da POC.
spark = (
    SparkSession.builder
    .appName("CaseCienciaDados")
    .master("local[*]")
    .getOrCreate()
)

# Define o nível de log como WARN para reduzir mensagens
# informativas do Spark e destacar avisos e erros.
spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 2. DEFINIÇÃO DOS SCHEMAS
# ============================================================

# Define explicitamente a estrutura e os tipos de dados
# esperados no arquivo compras.csv.
#
# A definição explícita do schema evita depender da inferência
# automática do Spark e garante maior controle sobre os tipos.
schema_compras = StructType([
    StructField("id_compra", IntegerType(), True),
    StructField("cliente_id", IntegerType(), True),
    StructField("data_compra", DateType(), True),
    StructField("valor_total", DecimalType(10, 2), True),
    StructField("metodo_pagamento", StringType(), True)
])


# Define explicitamente a estrutura e os tipos de dados
# esperados no arquivo pedidos.csv.
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

# Carrega o arquivo compras.csv utilizando:
# - primeira linha como cabeçalho;
# - codificação UTF-8;
# - formato de data yyyy-MM-dd;
# - schema definido anteriormente.
df_compras = (
    spark.read
    .option("header", True)
    .option("encoding", "UTF-8")
    .option("dateFormat", "yyyy-MM-dd")
    .schema(schema_compras)
    .csv("data/compras.csv")
)


# Carrega o arquivo pedidos.csv utilizando:
# - primeira linha como cabeçalho;
# - codificação UTF-8;
# - schema definido anteriormente.
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

# Exibe os dados originais de compras antes da aplicação
# de qualquer tratamento.
print("\n" + "=" * 70)
print("COMPRAS — DADOS ORIGINAIS")
print("=" * 70)

df_compras.show(truncate=False)


# Exibe os dados originais de pedidos antes da aplicação
# de qualquer tratamento.
print("\n" + "=" * 70)
print("PEDIDOS — DADOS ORIGINAIS")
print("=" * 70)

df_pedidos.show(truncate=False)


# Exibe o schema do DataFrame de compras para validar
# os nomes das colunas e os tipos definidos.
print("\n" + "=" * 70)
print("SCHEMA — COMPRAS")
print("=" * 70)

df_compras.printSchema()


# Exibe o schema do DataFrame de pedidos para validar
# os nomes das colunas e os tipos definidos.
print("\n" + "=" * 70)
print("SCHEMA — PEDIDOS")
print("=" * 70)

df_pedidos.printSchema()


# Conta a quantidade total de registros carregados antes
# da deduplicação.
qtd_compras = df_compras.count()
qtd_pedidos = df_pedidos.count()


# Exibe a quantidade de registros originais.
print("\n" + "=" * 70)
print("QUANTIDADE DE REGISTROS CARREGADOS")
print("=" * 70)

print(f"Compras: {qtd_compras}")
print(f"Pedidos: {qtd_pedidos}")


# ============================================================
# 5. IDENTIFICAÇÃO DOS REGISTROS DUPLICADOS
# ============================================================

# Agrupa os registros considerando todas as colunas do
# DataFrame de compras.
#
# A função count() contabiliza quantas vezes cada combinação
# completa de valores aparece.
#
# Registros com count maior que 1 representam linhas duplicadas.
duplicatas_compras = (
    df_compras
    .groupBy(*df_compras.columns)
    .count()
    .filter(F.col("count") > 1)
)


# Aplica a mesma lógica ao DataFrame de pedidos para identificar
# registros completamente duplicados.
duplicatas_pedidos = (
    df_pedidos
    .groupBy(*df_pedidos.columns)
    .count()
    .filter(F.col("count") > 1)
)


# Exibe as duplicidades encontradas no arquivo de compras.
# O orderBy é utilizado apenas para organizar a apresentação
# dos resultados no terminal.
print("\n" + "=" * 70)
print("COMPRAS — REGISTROS DUPLICADOS")
print("=" * 70)

duplicatas_compras.orderBy("id_compra").show(truncate=False)


# Exibe as duplicidades encontradas no arquivo de pedidos.
print("\n" + "=" * 70)
print("PEDIDOS — REGISTROS DUPLICADOS")
print("=" * 70)

duplicatas_pedidos.orderBy("id_pedido").show(truncate=False)


# ============================================================
# 6. DEDUPLICAÇÃO DOS DADOS
# ============================================================

# Remove registros completamente duplicados do DataFrame
# de compras, mantendo apenas uma ocorrência de cada linha.
#
# O DataFrame original é preservado e um novo DataFrame
# tratado é criado.
df_compras_dedup = df_compras.dropDuplicates()


# Remove registros completamente duplicados do DataFrame
# de pedidos.
df_pedidos_dedup = df_pedidos.dropDuplicates()


# Conta a quantidade de registros após o tratamento.
qtd_compras_dedup = df_compras_dedup.count()
qtd_pedidos_dedup = df_pedidos_dedup.count()


# Calcula quantos registros foram removidos durante
# o processo de deduplicação.
duplicatas_removidas_compras = (
    qtd_compras - qtd_compras_dedup
)

duplicatas_removidas_pedidos = (
    qtd_pedidos - qtd_pedidos_dedup
)


# Exibe o DataFrame de compras após a remoção das duplicidades.
# A ordenação é utilizada somente para facilitar a visualização.
print("\n" + "=" * 70)
print("COMPRAS — APÓS DEDUPLICAÇÃO")
print("=" * 70)

df_compras_dedup.orderBy("id_compra").show(truncate=False)


# Exibe o DataFrame de pedidos após a remoção das duplicidades.
print("\n" + "=" * 70)
print("PEDIDOS — APÓS DEDUPLICAÇÃO")
print("=" * 70)

df_pedidos_dedup.orderBy("id_pedido").show(truncate=False)


# Apresenta um resumo comparando a quantidade de registros
# antes e depois da deduplicação.
print("\n" + "=" * 70)
print("RESUMO DA DEDUPLICAÇÃO")
print("=" * 70)

print(
    f"Compras: {qtd_compras} registros antes | "
    f"{qtd_compras_dedup} depois | "
    f"{duplicatas_removidas_compras} duplicados removidos"
)

print(
    f"Pedidos: {qtd_pedidos} registros antes | "
    f"{qtd_pedidos_dedup} depois | "
    f"{duplicatas_removidas_pedidos} duplicados removidos"
)


# ============================================================
# 7. VALIDAÇÃO DOS VALORES DAS COMPRAS
# ============================================================

# Calcula o valor total de cada item de pedido multiplicando
# a quantidade vendida pelo preço unitário do produto.
#
# Exemplo:
# quantidade = 2
# preco_unitario = 50.00
# valor_item = 100.00
df_pedidos_com_valor = (
    df_pedidos_dedup
    .withColumn(
        "valor_item",
        F.col("quantidade") * F.col("preco_unitario")
    )
)


# Exibe os pedidos com o valor calculado de cada item.
print("\n" + "=" * 70)
print("PEDIDOS — VALOR CALCULADO DOS ITENS")
print("=" * 70)

df_pedidos_com_valor.orderBy("id_pedido").show(truncate=False)


# Agrupa os pedidos pelo identificador da compra e soma
# os valores dos itens associados a cada compra.
#
# Dessa forma, calculamos quanto cada compra deveria valer
# considerando os pedidos registrados.
df_total_pedidos_por_compra = (
    df_pedidos_com_valor
    .groupBy("id_compra")
    .agg(
        F.sum("valor_item").alias("valor_total_pedidos")
    )
)


# Exibe o valor total calculado dos pedidos para cada compra.
print("\n" + "=" * 70)
print("TOTAL DOS PEDIDOS POR COMPRA")
print("=" * 70)

df_total_pedidos_por_compra.orderBy("id_compra").show(truncate=False)


# Relaciona as compras com os valores calculados dos pedidos
# utilizando o campo id_compra.
#
# O LEFT JOIN é utilizado para manter todas as compras,
# inclusive aquelas que não possuem pedidos associados.
#
# Se fosse utilizado INNER JOIN, uma compra sem pedidos
# desapareceria do resultado e a inconsistência poderia
# deixar de ser identificada.
df_validacao_compras = (
    df_compras_dedup
    .join(
        df_total_pedidos_por_compra,
        on="id_compra",
        how="left"
    )
)


# Calcula a diferença entre o valor registrado no arquivo
# de compras e o valor calculado a partir dos pedidos.
#
# Quando não existem pedidos associados, o valor calculado
# permanece nulo e a diferença também permanece nula.
df_validacao_compras = (
    df_validacao_compras
    .withColumn(
        "diferenca",
        F.col("valor_total") - F.col("valor_total_pedidos")
    )
)


# Classifica cada compra de acordo com o resultado da validação.
#
# SEM PEDIDOS:
# a compra existe no arquivo compras.csv, mas não possui
# registros correspondentes em pedidos.csv.
#
# OK:
# o valor registrado em compras.csv é igual ao valor
# calculado com base nos pedidos.
#
# INCONSISTENTE:
# existem pedidos associados, mas os valores não coincidem.
df_validacao_compras = (
    df_validacao_compras
    .withColumn(
        "status_validacao",
        F.when(
            F.col("valor_total_pedidos").isNull(),
            F.lit("SEM PEDIDOS")
        )
        .when(
            F.col("valor_total") == F.col("valor_total_pedidos"),
            F.lit("OK")
        )
        .otherwise(
            F.lit("INCONSISTENTE")
        )
    )
)


# Seleciona somente as colunas mais relevantes para
# apresentação do resultado da validação.
df_validacao_compras = (
    df_validacao_compras
    .select(
        "id_compra",
        "cliente_id",
        "data_compra",
        "valor_total",
        "valor_total_pedidos",
        "diferenca",
        "status_validacao"
    )
)


# Exibe a comparação entre o valor registrado de cada compra
# e o valor calculado a partir dos pedidos.
print("\n" + "=" * 70)
print("VALIDAÇÃO — VALOR DA COMPRA X TOTAL DOS PEDIDOS")
print("=" * 70)

df_validacao_compras.orderBy("id_compra").show(truncate=False)


# Filtra somente as compras que apresentaram algum problema,
# seja divergência financeira ou ausência de pedidos.
df_inconsistencias = (
    df_validacao_compras
    .filter(F.col("status_validacao") != "OK")
)


# Exibe somente as inconsistências encontradas.
print("\n" + "=" * 70)
print("INCONSISTÊNCIAS IDENTIFICADAS")
print("=" * 70)

df_inconsistencias.orderBy("id_compra").show(truncate=False)


# Gera um resumo da quantidade de registros por status,
# facilitando a interpretação do resultado da validação.
df_resumo_validacao = (
    df_validacao_compras
    .groupBy("status_validacao")
    .count()
)


print("\n" + "=" * 70)
print("RESUMO DA VALIDAÇÃO")
print("=" * 70)

df_resumo_validacao.orderBy("status_validacao").show(truncate=False)


# ============================================================
# 8. ENCERRAMENTO
# ============================================================

# Encerra a sessão Spark e libera os recursos utilizados
# durante a execução da aplicação.
spark.stop()