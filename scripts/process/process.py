from pyspark.sql import SparkSession, dataframe
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql import functions as f
import os
import re

spark = SparkSession.builder.master("local[*]")\
    .enableHiveSupport()\
    .getOrCreate()

# Criando dataframes diretamente do Hive
df_clientes = spark.sql("SELECT * FROM DESAFIO_CURSO.TBL_CLIENTES")
df_divisao = spark.sql("SELECT * FROM DESAFIO_CURSO.TBL_DIVISAO")
df_endereco = spark.sql("SELECT * FROM DESAFIO_CURSO.TBL_ENDERECO")
df_regiao = spark.sql("SELECT * FROM DESAFIO_CURSO.TBL_REGIAO")
df_vendas = spark.sql("SELECT * FROM DESAFIO_CURSO.TBL_VENDAS")

#1 Espaço para tratar e juntar os campos e a criação do modelo dimensional
#1.1 Criação das tabelas temporárias para tratar os dados com SQL

df_clientes.createOrReplaceTempView("clientes")
df_divisao.createOrReplaceTempView("divisao")
df_endereco.createOrReplaceTempView("endereco")
df_regiao.createOrReplaceTempView("regiao")
df_vendas.createOrReplaceTempView("vendas")

#1.2 Criação da tabela stage

df_stage = spark.sql(
'''
select * from vendas v
inner join clientes c on c.CustomerKey = v.CustomerKey
left join endereco e on e.`Address Number` = c.`Address Number`
left join divisao d on c.Division = d.Division
left join regiao r on c.`Region Code` = r.`Region Code`

'''
).show(10)

# criando o fato
ft_vendas = []

#criando as dimensões
dim_clientes = []
dim_endereco = []
dim_regiao = []
dim_divisao = []

# função para salvar os dados
def salvar_df(df, file):
    output = "/input/desafio_hive/gold/" + file
    erase = "hdfs dfs -rm " + output + "/*"
    rename = "hdfs dfs -get /datalake/gold/"+file+"/part-* /input/desafio_hive/gold/"+file+".csv"
    print(rename)
    
    
    df.coalesce(1).write\
        .format("csv")\
        .option("header", True)\
        .option("delimiter", ";")\
        .mode("overwrite")\
        .save("/datalake/gold/"+file+"/")

    os.system(erase)
    os.system(rename)

salvar_df(dim_clientes, 'dimclientes')