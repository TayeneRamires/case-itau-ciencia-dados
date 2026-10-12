# Biblioteca utilizada para configurar o ambiente Python
import os
import sys

# Utilizada para comparar valores monetários com precisão.
from decimal import Decimal

# Gerenciamento da sessão Spark e funções utilizadas
# nas validações dos DataFrames.
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Tipos utilizados para definir os schemas dos arquivos CSV.
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DateType,
    DecimalType
)


# Configura o PySpark para utilizar o mesmo interpretador Python
# utilizado pelo ambiente virtual atual.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


# ============================================================
# FUNÇÃO AUXILIAR
# ============================================================

def teste_ok(mensagem):
    """
    Exibe no terminal uma mensagem indicando que
    determinada validação foi concluída com sucesso.
    """
    print(f"[OK] {mensagem}")


# ============================================================
# EXECUÇÃO DOS TESTES
# ============================================================

def main():

    # ========================================================
    # 1. INICIALIZAÇÃO DO SPARK
    # ========================================================

    spark = (
        SparkSession.builder
        .appName("TestesQualidadeCaseCienciaDados")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:

        print("\n" + "=" * 70)
        print("TESTES DE QUALIDADE DOS DADOS")
        print("=" * 70)


        # ====================================================
        # 2. DEFINIÇÃO DOS SCHEMAS
        # ====================================================

        schema_compras = StructType([
            StructField("id_compra", IntegerType(), True),
            StructField("cliente_id", IntegerType(), True),
            StructField("data_compra", DateType(), True),
            StructField("valor_total", DecimalType(10, 2), True),
            StructField("metodo_pagamento", StringType(), True)
        ])


        schema_pedidos = StructType([
            StructField("id_pedido", IntegerType(), True),
            StructField("id_compra", IntegerType(), True),
            StructField("produto_id", StringType(), True),
            StructField("quantidade", IntegerType(), True),
            StructField("preco_unitario", DecimalType(10, 2), True)
        ])


        # ====================================================
        # 3. LEITURA DOS DADOS
        # ====================================================

        df_compras = (
            spark.read
            .option("header", True)
            .option("encoding", "UTF-8")
            .option("dateFormat", "yyyy-MM-dd")
            .schema(schema_compras)
            .csv("data/compras.csv")
        )


        df_pedidos = (
            spark.read
            .option("header", True)
            .option("encoding", "UTF-8")
            .schema(schema_pedidos)
            .csv("data/pedidos.csv")
        )


        # ====================================================
        # 4. DEDUPLICAÇÃO
        # ====================================================

        qtd_compras_originais = df_compras.count()
        qtd_pedidos_originais = df_pedidos.count()

        df_compras_dedup = df_compras.dropDuplicates()
        df_pedidos_dedup = df_pedidos.dropDuplicates()

        qtd_compras_dedup = df_compras_dedup.count()
        qtd_pedidos_dedup = df_pedidos_dedup.count()


        # Verifica se os registros duplicados foram removidos.
        assert qtd_compras_originais == 7, (
            "Quantidade original de compras diferente do esperado."
        )

        assert qtd_compras_dedup == 5, (
            "A deduplicação de compras não produziu 5 registros."
        )

        assert (
            qtd_compras_dedup
            == df_compras_dedup.dropDuplicates().count()
        ), "Ainda existem duplicidades em compras."

        teste_ok(
            "Deduplicação de compras validada: "
            "7 registros originais, 5 únicos e 2 duplicados removidos."
        )


        assert qtd_pedidos_originais == 7, (
            "Quantidade original de pedidos diferente do esperado."
        )

        assert qtd_pedidos_dedup == 5, (
            "A deduplicação de pedidos não produziu 5 registros."
        )

        assert (
            qtd_pedidos_dedup
            == df_pedidos_dedup.dropDuplicates().count()
        ), "Ainda existem duplicidades em pedidos."

        teste_ok(
            "Deduplicação de pedidos validada: "
            "7 registros originais, 5 únicos e 2 duplicados removidos."
        )


        # ====================================================
        # 5. VALIDAÇÃO DE CAMPOS OBRIGATÓRIOS
        # ====================================================

        campos_obrigatorios_compras = [
            "id_compra",
            "cliente_id",
            "data_compra",
            "valor_total",
            "metodo_pagamento"
        ]

        for campo in campos_obrigatorios_compras:

            qtd_nulos = (
                df_compras_dedup
                .filter(F.col(campo).isNull())
                .count()
            )

            assert qtd_nulos == 0, (
                f"Foram encontrados valores NULL em compras.{campo}"
            )

        teste_ok(
            "Campos obrigatórios de compras não possuem valores NULL."
        )


        campos_obrigatorios_pedidos = [
            "id_pedido",
            "id_compra",
            "produto_id",
            "quantidade",
            "preco_unitario"
        ]

        for campo in campos_obrigatorios_pedidos:

            qtd_nulos = (
                df_pedidos_dedup
                .filter(F.col(campo).isNull())
                .count()
            )

            assert qtd_nulos == 0, (
                f"Foram encontrados valores NULL em pedidos.{campo}"
            )

        teste_ok(
            "Campos obrigatórios de pedidos não possuem valores NULL."
        )


        # ====================================================
        # 6. VALIDAÇÃO DE VALORES NUMÉRICOS
        # ====================================================

        compras_valor_invalido = (
            df_compras_dedup
            .filter(F.col("valor_total") < 0)
            .count()
        )

        assert compras_valor_invalido == 0, (
            "Foram encontradas compras com valor_total negativo."
        )

        teste_ok(
            "Não existem compras com valor_total negativo."
        )


        pedidos_quantidade_invalida = (
            df_pedidos_dedup
            .filter(F.col("quantidade") <= 0)
            .count()
        )

        assert pedidos_quantidade_invalida == 0, (
            "Foram encontrados pedidos com quantidade menor ou igual a zero."
        )


        pedidos_preco_invalido = (
            df_pedidos_dedup
            .filter(F.col("preco_unitario") < 0)
            .count()
        )

        assert pedidos_preco_invalido == 0, (
            "Foram encontrados pedidos com preco_unitario negativo."
        )

        teste_ok(
            "Quantidades e preços dos pedidos possuem valores válidos."
        )


        # ====================================================
        # 7. INTEGRIDADE ENTRE PEDIDOS E COMPRAS
        # ====================================================

        # Procura pedidos cujo id_compra não exista
        # no arquivo de compras.
        pedidos_orfaos = (
            df_pedidos_dedup
            .join(
                df_compras_dedup.select("id_compra"),
                on="id_compra",
                how="left_anti"
            )
        )

        qtd_pedidos_orfaos = pedidos_orfaos.count()

        assert qtd_pedidos_orfaos == 0, (
            "Existem pedidos associados a compras inexistentes."
        )

        teste_ok(
            "Todos os pedidos estão associados a compras existentes."
        )


        # ====================================================
        # 8. COMPRAS SEM PEDIDOS
        # ====================================================

        # Procura compras que não possuem nenhum pedido associado.
        compras_sem_pedidos = (
            df_compras_dedup
            .join(
                df_pedidos_dedup
                .select("id_compra")
                .distinct(),
                on="id_compra",
                how="left_anti"
            )
        )

        ids_compras_sem_pedidos = sorted([
            linha["id_compra"]
            for linha in compras_sem_pedidos.collect()
        ])

        assert ids_compras_sem_pedidos == [5], (
            "A compra sem pedidos encontrada é diferente da esperada."
        )

        teste_ok(
            "Compra sem pedidos identificada corretamente: id_compra = 5."
        )


        # ====================================================
        # 9. VALIDAÇÃO FINANCEIRA
        # ====================================================

        # Calcula o valor de cada item dos pedidos.
        df_pedidos_com_valor = (
            df_pedidos_dedup
            .withColumn(
                "valor_item",
                F.col("quantidade") * F.col("preco_unitario")
            )
        )


        # Soma o valor dos itens por compra.
        df_total_pedidos_por_compra = (
            df_pedidos_com_valor
            .groupBy("id_compra")
            .agg(
                F.sum("valor_item").alias("valor_total_pedidos")
            )
        )


        # Compara somente compras que possuem pedidos associados.
        df_validacao_financeira = (
            df_compras_dedup
            .join(
                df_total_pedidos_por_compra,
                on="id_compra",
                how="inner"
            )
        )


        divergencias_financeiras = (
            df_validacao_financeira
            .filter(
                F.col("valor_total")
                != F.col("valor_total_pedidos")
            )
        )

        qtd_divergencias = divergencias_financeiras.count()

        assert qtd_divergencias == 0, (
            "Foram encontradas divergências financeiras "
            "entre compras e pedidos."
        )

        teste_ok(
            "Valores das compras com pedidos associados "
            "estão financeiramente consistentes."
        )


        # ====================================================
        # 10. TESTE DA ANÁLISE POR CLIENTE
        # ====================================================

        df_compras_por_cliente = (
            df_compras_dedup
            .groupBy("cliente_id")
            .agg(
                F.countDistinct("id_compra").alias(
                    "quantidade_compras"
                )
            )
        )


        compras_por_cliente = {
            linha["cliente_id"]: linha["quantidade_compras"]
            for linha in df_compras_por_cliente.collect()
        }


        resultado_esperado_clientes = {
            101: 2,
            102: 1,
            103: 1,
            104: 1
        }


        assert compras_por_cliente == resultado_esperado_clientes, (
            "A quantidade de compras por cliente "
            "é diferente do resultado esperado."
        )

        teste_ok(
            "Agrupamento de compras por cliente validado."
        )


        # ====================================================
        # 11. TESTE DO MÉTODO DE PAGAMENTO
        # ====================================================

        df_pagamentos = (
            df_compras_dedup
            .groupBy("metodo_pagamento")
            .agg(
                F.countDistinct("id_compra").alias(
                    "quantidade_compras"
                )
            )
        )


        maior_quantidade_pagamento = (
            df_pagamentos
            .agg(
                F.max("quantidade_compras").alias(
                    "maior_quantidade"
                )
            )
            .collect()[0]["maior_quantidade"]
        )


        metodos_mais_utilizados = sorted([
            linha["metodo_pagamento"]
            for linha in df_pagamentos
            .filter(
                F.col("quantidade_compras")
                == maior_quantidade_pagamento
            )
            .collect()
        ])


        assert metodos_mais_utilizados == ["Cartão de Crédito"], (
            "O método de pagamento mais utilizado "
            "é diferente do esperado."
        )

        assert maior_quantidade_pagamento == 3, (
            "A quantidade de utilizações do método "
            "mais utilizado é diferente do esperado."
        )

        teste_ok(
            "Método de pagamento mais utilizado validado: "
            "Cartão de Crédito com 3 compras."
        )


        # ====================================================
        # 12. TESTE DOS PRODUTOS MAIS VENDIDOS
        # ====================================================

        df_quantidade_por_produto = (
            df_pedidos_dedup
            .groupBy("produto_id")
            .agg(
                F.sum("quantidade").alias(
                    "quantidade_total_vendida"
                )
            )
        )


        maior_quantidade_produto = (
            df_quantidade_por_produto
            .agg(
                F.max("quantidade_total_vendida").alias(
                    "maior_quantidade"
                )
            )
            .collect()[0]["maior_quantidade"]
        )


        produtos_mais_vendidos = sorted([
            linha["produto_id"]
            for linha in df_quantidade_por_produto
            .filter(
                F.col("quantidade_total_vendida")
                == maior_quantidade_produto
            )
            .collect()
        ])


        assert produtos_mais_vendidos == ["A003"], (
            "O produto mais vendido é diferente do esperado."
        )

        assert maior_quantidade_produto == 4, (
            "A quantidade do produto mais vendido "
            "é diferente do esperado."
        )

        teste_ok(
            "Produto mais vendido validado: A003 com 4 unidades."
        )


        # ====================================================
        # 13. TESTE DO FATURAMENTO POR DATA
        # ====================================================

        df_faturamento_por_data = (
            df_compras_dedup
            .groupBy("data_compra")
            .agg(
                F.sum("valor_total").alias(
                    "faturamento_total_registrado"
                )
            )
        )


        maior_faturamento = (
            df_faturamento_por_data
            .agg(
                F.max("faturamento_total_registrado").alias(
                    "maior_faturamento"
                )
            )
            .collect()[0]["maior_faturamento"]
        )


        datas_maior_faturamento = [
            linha["data_compra"].isoformat()
            for linha in df_faturamento_por_data
            .filter(
                F.col("faturamento_total_registrado")
                == maior_faturamento
            )
            .collect()
        ]


        assert sorted(datas_maior_faturamento) == ["2023-10-05"], (
            "A data de maior faturamento "
            "é diferente do resultado esperado."
        )

        assert maior_faturamento == Decimal("500.00"), (
            "O valor do maior faturamento "
            "é diferente do resultado esperado."
        )

        teste_ok(
            "Data de maior faturamento validada: "
            "2023-10-05 com R$ 500,00 registrados."
        )


        # ====================================================
        # 14. RESULTADO FINAL
        # ====================================================

        print("\n" + "=" * 70)
        print("TODOS OS TESTES FORAM EXECUTADOS COM SUCESSO")
        print("=" * 70)

        print(
            "\nInconsistência conhecida e documentada: "
            "a compra 5 possui valor registrado de R$ 500,00, "
            "mas não possui pedidos associados."
        )


    # Garante que a sessão Spark seja encerrada
    # mesmo se algum teste falhar.
    finally:

        spark.stop()


# Executa os testes apenas quando este arquivo
# é executado diretamente.
if __name__ == "__main__":
    main()