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


# Exibe o schema do DataFrame de compras.
print("\n" + "=" * 70)
print("SCHEMA — COMPRAS")
print("=" * 70)

df_compras.printSchema()


# Exibe o schema do DataFrame de pedidos.
print("\n" + "=" * 70)
print("SCHEMA — PEDIDOS")
print("=" * 70)

df_pedidos.printSchema()


# Conta os registros originais antes de qualquer tratamento.
qtd_compras = df_compras.count()
qtd_pedidos = df_pedidos.count()


# Exibe a quantidade de registros carregados.
print("\n" + "=" * 70)
print("QUANTIDADE DE REGISTROS CARREGADOS")
print("=" * 70)

print(f"Compras: {qtd_compras}")
print(f"Pedidos: {qtd_pedidos}")


# ============================================================
# 5. IDENTIFICAÇÃO DOS REGISTROS DUPLICADOS
# ============================================================

# Agrupa os registros considerando todas as colunas do DataFrame.
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


# Aplica a mesma lógica ao DataFrame de pedidos.
duplicatas_pedidos = (
    df_pedidos
    .groupBy(*df_pedidos.columns)
    .count()
    .filter(F.col("count") > 1)
)


# Exibe as duplicidades encontradas em compras.
print("\n" + "=" * 70)
print("COMPRAS — REGISTROS DUPLICADOS")
print("=" * 70)

duplicatas_compras.orderBy("id_compra").show(truncate=False)


# Exibe as duplicidades encontradas em pedidos.
print("\n" + "=" * 70)
print("PEDIDOS — REGISTROS DUPLICADOS")
print("=" * 70)

duplicatas_pedidos.orderBy("id_pedido").show(truncate=False)


# ============================================================
# 6. DEDUPLICAÇÃO DOS DADOS
# ============================================================

# Remove registros completamente duplicados do DataFrame
# de compras, mantendo apenas uma ocorrência de cada linha.
df_compras_dedup = df_compras.dropDuplicates()


# Remove registros completamente duplicados do DataFrame
# de pedidos.
df_pedidos_dedup = df_pedidos.dropDuplicates()


# Conta novamente os registros após a deduplicação.
qtd_compras_dedup = df_compras_dedup.count()
qtd_pedidos_dedup = df_pedidos_dedup.count()


# Calcula quantos registros foram removidos.
duplicatas_removidas_compras = (
    qtd_compras - qtd_compras_dedup
)

duplicatas_removidas_pedidos = (
    qtd_pedidos - qtd_pedidos_dedup
)


# Exibe as compras após a deduplicação.
print("\n" + "=" * 70)
print("COMPRAS — APÓS DEDUPLICAÇÃO")
print("=" * 70)

df_compras_dedup.orderBy("id_compra").show(truncate=False)


# Exibe os pedidos após a deduplicação.
print("\n" + "=" * 70)
print("PEDIDOS — APÓS DEDUPLICAÇÃO")
print("=" * 70)

df_pedidos_dedup.orderBy("id_pedido").show(truncate=False)


# Exibe um resumo do processo de deduplicação.
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
# a quantidade pelo preço unitário.
#
# Exemplo:
# 2 unidades x R$ 50,00 = R$ 100,00.
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
# o valor dos itens relacionados a cada compra.
df_total_pedidos_por_compra = (
    df_pedidos_com_valor
    .groupBy("id_compra")
    .agg(
        F.sum("valor_item").alias("valor_total_pedidos")
    )
)


# Exibe o total calculado dos pedidos por compra.
print("\n" + "=" * 70)
print("TOTAL DOS PEDIDOS POR COMPRA")
print("=" * 70)

df_total_pedidos_por_compra.orderBy("id_compra").show(truncate=False)


# Relaciona as compras com os valores calculados dos pedidos
# utilizando o campo id_compra.
#
# O LEFT JOIN mantém todas as compras, inclusive aquelas
# que não possuem pedidos associados.
#
# Isso permite identificar compras existentes em compras.csv
# que não possuem correspondência em pedidos.csv.
df_validacao_compras = (
    df_compras_dedup
    .join(
        df_total_pedidos_por_compra,
        on="id_compra",
        how="left"
    )
)


# Calcula a diferença entre o valor registrado na compra
# e o valor calculado com base nos pedidos.
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
# a compra existe, mas não possui pedidos associados.
#
# OK:
# o valor registrado é igual ao valor calculado.
#
# INCONSISTENTE:
# existem pedidos associados, mas os valores são diferentes.
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


# Mantém somente as colunas relevantes para a apresentação
# do resultado da validação.
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


# Exibe a comparação entre o valor registrado na compra
# e o valor calculado a partir dos pedidos.
print("\n" + "=" * 70)
print("VALIDAÇÃO — VALOR DA COMPRA X TOTAL DOS PEDIDOS")
print("=" * 70)

df_validacao_compras.orderBy("id_compra").show(truncate=False)


# Filtra somente as compras que apresentaram algum problema.
df_inconsistencias = (
    df_validacao_compras
    .filter(F.col("status_validacao") != "OK")
)


# Exibe somente as inconsistências identificadas.
print("\n" + "=" * 70)
print("INCONSISTÊNCIAS IDENTIFICADAS")
print("=" * 70)

df_inconsistencias.orderBy("id_compra").show(truncate=False)


# Gera um resumo da quantidade de compras por status
# de validação.
df_resumo_validacao = (
    df_validacao_compras
    .groupBy("status_validacao")
    .count()
)


# Exibe o resumo da validação.
print("\n" + "=" * 70)
print("RESUMO DA VALIDAÇÃO")
print("=" * 70)

df_resumo_validacao.orderBy("status_validacao").show(truncate=False)


# ============================================================
# 8. ANÁLISE DE COMPRAS
# ============================================================

# Agrupa os dados de compras pelo cliente.
#
# Para cada cliente são calculados:
# - quantidade de compras distintas realizadas;
# - valor total registrado no arquivo compras.csv.
#
# O termo "registrado" é utilizado porque a etapa anterior
# identificou que uma compra pode existir no arquivo de compras
# sem possuir pedidos associados.
#
# Também é utilizado o DataFrame deduplicado para impedir que
# registros repetidos distorçam os resultados.
df_compras_por_cliente = (
    df_compras_dedup
    .groupBy("cliente_id")
    .agg(
        F.countDistinct("id_compra").alias("quantidade_compras"),
        F.sum("valor_total").alias("valor_total_registrado")
    )
)


# Exibe a quantidade de compras e o valor total registrado
# para cada cliente.
print("\n" + "=" * 70)
print("ANÁLISE DE COMPRAS — TOTAL POR CLIENTE")
print("=" * 70)

df_compras_por_cliente.orderBy("cliente_id").show(truncate=False)


# Agrupa as compras pelo método de pagamento e contabiliza
# quantas compras distintas utilizaram cada método.
df_compras_por_pagamento = (
    df_compras_dedup
    .groupBy("metodo_pagamento")
    .agg(
        F.countDistinct("id_compra").alias("quantidade_compras")
    )
)


# Exibe todos os métodos de pagamento ordenados da maior
# para a menor quantidade de compras.
#
# Em caso de empate na quantidade, os métodos são ordenados
# alfabeticamente apenas para facilitar a visualização.
print("\n" + "=" * 70)
print("ANÁLISE DE COMPRAS — MÉTODOS DE PAGAMENTO")
print("=" * 70)

df_compras_por_pagamento.orderBy(
    F.desc("quantidade_compras"),
    F.asc("metodo_pagamento")
).show(truncate=False)


# Calcula qual foi a maior quantidade de compras encontrada
# entre os métodos de pagamento.
df_maior_quantidade_pagamento = (
    df_compras_por_pagamento
    .agg(
        F.max("quantidade_compras").alias("maior_quantidade")
    )
)


# Relaciona a quantidade de compras por método com o maior
# valor encontrado.
#
# Dessa forma, se dois ou mais métodos tiverem a mesma
# quantidade máxima de compras, todos serão apresentados,
# tratando corretamente um possível empate.
df_metodo_mais_utilizado = (
    df_compras_por_pagamento
    .join(
        df_maior_quantidade_pagamento,
        df_compras_por_pagamento["quantidade_compras"]
        == df_maior_quantidade_pagamento["maior_quantidade"],
        how="inner"
    )
    .select(
        "metodo_pagamento",
        "quantidade_compras"
    )
)


# Exibe o método de pagamento mais utilizado.
print("\n" + "=" * 70)
print("MÉTODO DE PAGAMENTO MAIS UTILIZADO")
print("=" * 70)

df_metodo_mais_utilizado.orderBy(
    "metodo_pagamento"
).show(truncate=False)


# ============================================================
# 9. PRODUTOS MAIS VENDIDOS
# ============================================================

# Agrupa os pedidos pelo identificador do produto.
#
# Para cada produto é somada a quantidade vendida em todos
# os pedidos existentes após a deduplicação.
#
# É utilizado o DataFrame de pedidos deduplicado para evitar
# que registros repetidos aumentem artificialmente a quantidade
# vendida de determinado produto.
df_quantidade_por_produto = (
    df_pedidos_dedup
    .groupBy("produto_id")
    .agg(
        F.sum("quantidade").alias("quantidade_total_vendida")
    )
)


# Exibe o ranking completo dos produtos, ordenando da maior
# para a menor quantidade total vendida.
#
# Em caso de empate, produto_id é utilizado como segundo
# critério somente para manter a apresentação organizada.
print("\n" + "=" * 70)
print("PRODUTOS MAIS VENDIDOS — RANKING")
print("=" * 70)

df_quantidade_por_produto.orderBy(
    F.desc("quantidade_total_vendida"),
    F.asc("produto_id")
).show(truncate=False)


# Identifica a maior quantidade total vendida encontrada
# entre todos os produtos.
df_maior_quantidade_produto = (
    df_quantidade_por_produto
    .agg(
        F.max("quantidade_total_vendida").alias(
            "maior_quantidade_vendida"
        )
    )
)


# Seleciona o produto ou os produtos que possuem a maior
# quantidade total vendida.
#
# Essa abordagem permite tratar corretamente situações
# de empate, pois todos os produtos que atingirem a maior
# quantidade serão apresentados.
df_produto_mais_vendido = (
    df_quantidade_por_produto
    .join(
        df_maior_quantidade_produto,
        df_quantidade_por_produto["quantidade_total_vendida"]
        == df_maior_quantidade_produto["maior_quantidade_vendida"],
        how="inner"
    )
    .select(
        "produto_id",
        "quantidade_total_vendida"
    )
)


# Exibe explicitamente o produto ou os produtos
# com a maior quantidade total vendida.
print("\n" + "=" * 70)
print("PRODUTO MAIS VENDIDO")
print("=" * 70)

df_produto_mais_vendido.orderBy(
    "produto_id"
).show(truncate=False)


# ============================================================
# 10. DATAS COM MAIOR FATURAMENTO
# ============================================================

# Agrupa as compras deduplicadas pela data em que foram realizadas.
#
# Para cada data é somado o valor_total registrado no arquivo
# compras.csv, obtendo o faturamento registrado daquele dia.
#
# É utilizado o DataFrame deduplicado para evitar que compras
# repetidas aumentem artificialmente o faturamento.
df_faturamento_por_data = (
    df_compras_dedup
    .groupBy("data_compra")
    .agg(
        F.sum("valor_total").alias("faturamento_total_registrado")
    )
)


# Exibe o faturamento de todas as datas, ordenando do maior
# para o menor valor.
#
# O termo "registrado" é utilizado porque o cálculo considera
# os valores existentes em compras.csv. A validação anterior
# já identificou separadamente compras sem pedidos associados.
print("\n" + "=" * 70)
print("FATURAMENTO REGISTRADO POR DATA")
print("=" * 70)

df_faturamento_por_data.orderBy(
    F.desc("faturamento_total_registrado"),
    F.asc("data_compra")
).show(truncate=False)


# Identifica o maior faturamento diário registrado
# entre todas as datas existentes no arquivo.
df_maior_faturamento = (
    df_faturamento_por_data
    .agg(
        F.max("faturamento_total_registrado").alias(
            "maior_faturamento"
        )
    )
)


# Seleciona a data ou as datas que possuem o maior
# faturamento registrado.
#
# Essa abordagem permite tratar corretamente uma situação
# de empate, exibindo todas as datas que possuam o mesmo
# maior faturamento.
df_datas_maior_faturamento = (
    df_faturamento_por_data
    .join(
        df_maior_faturamento,
        df_faturamento_por_data["faturamento_total_registrado"]
        == df_maior_faturamento["maior_faturamento"],
        how="inner"
    )
    .select(
        "data_compra",
        "faturamento_total_registrado"
    )
)


# Exibe explicitamente a data ou as datas com
# o maior faturamento registrado.
print("\n" + "=" * 70)
print("DATA COM MAIOR FATURAMENTO")
print("=" * 70)

df_datas_maior_faturamento.orderBy(
    "data_compra"
).show(truncate=False)


# ============================================================
# 11. ENCERRAMENTO
# ============================================================

# Encerra a sessão Spark e libera os recursos utilizados
# durante a execução da aplicação.
spark.stop()