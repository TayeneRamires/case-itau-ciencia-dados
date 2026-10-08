import os
import sys

# Configura o PySpark para usar o mesmo Python
# que está executando este projeto.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession


print("=== INICIANDO TESTE DO SPARK ===")


spark = (
    SparkSession.builder
    .appName("TesteAmbienteCaseItau")
    .master("local[*]")
    .getOrCreate()
)


spark.sparkContext.setLogLevel("WARN")


dados = [
    (1, "Tayene"),
    (2, "Spark"),
    (3, "Itaú"),
]


df = spark.createDataFrame(
    dados,
    ["id", "nome"]
)


print("\n=== DATAFRAME DE TESTE ===")

df.show()


print("\n=== VERSÃO DO SPARK ===")

print(spark.version)


spark.stop()


print("\n=== TESTE FINALIZADO COM SUCESSO ===")