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

# Espaço para tratar e juntar os campos e a criação do modelo dimensional

# Campos strings vazios deverão ser preenchidos com NÃO INFORMADO
# LIMPANDO VAZIOS DE CLIENTES
df_clientes = df_clientes.select([when(col(c)=="   ","Não informado").otherwise(col(c)).alias(c) for c in df_clientes.columns])
# LIMPANDO VAZIOS DE ENDERECO
df_endereco = df_endereco.select([when(col(c)=="                                        ","Não informado").otherwise(col(c)).alias(c) for c in df_endereco.columns])
df_endereco = df_endereco.select([when(col(c)=="","Não informado").otherwise(col(c)).alias(c) for c in df_endereco.columns])
df_endereco = df_endereco.select([when(col(c)=="                         ","Não informado").otherwise(col(c)).alias(c) for c in df_endereco.columns])
df_endereco = df_endereco.select([when(col(c)=="            ","Não informado").otherwise(col(c)).alias(c) for c in df_endereco.columns])
# LIMPANDO VAZIOS DE VENDAS
df_vendas = df_vendas.select([when(col(c)=="","Não informado").otherwise(col(c)).alias(c) for c in df_vendas.columns])

# TABELAS TEMPORARIAS 
df_clientes.createOrReplaceTempView("clientes")
df_divisao.createOrReplaceTempView("divisao")
df_endereco.createOrReplaceTempView("endereco")
df_regiao.createOrReplaceTempView("regiao")
df_vendas.createOrReplaceTempView("vendas")

# TABELA STAGE 
df_stage = spark.sql(
'''
select * from vendas v
inner join clientes c on c.CustomerKey = v.CustomerKey
left join endereco e on e.`Address Number` = c.`Address Number`
left join divisao d on c.Division = d.Division
left join regiao r on c.`Region Code` = r.`Region Code`

'''
).show(10)

# Criação das PKS na tabela stage

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