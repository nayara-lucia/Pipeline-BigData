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
df_stage = df_vendas.alias('v') \
.join(df_clientes.alias('c'), on='customerkey',how='inner') \
.join(df_divisao.alias('d'), on='division', how='left') \
.join(df_endereco.alias('e'), on='address number', how='left') \
.join(df_regiao.alias('r'), on='region code', how='left') \

# Criação das PKS na tabela stage
df_stage = df_stage.withColumn('PK_CLIENTE', sha2(concat_ws('_', df_stage['Address Number'], df_stage['Business Family'],df_stage['Business Unit'],df_stage['Customer'],df_stage['CustomerKey'],df_stage['Customer Type'],df_stage['Division'],df_stage['Line of Business'],df_stage['Phone'],df_stage['Region Code'],df_stage['Regional Sales Mgr'],df_stage['Search Type']), 256))

df_stage = df_stage.withColumn('PK_LOCAL', sha2(concat_ws('_', df_stage['Region Code'], df_stage['Region Name'],df_stage['Division'], df_stage['Division Name'],df_stage['Address Number'], df_stage['City'], df_stage['Country'],df_stage['Customer Address 1'],df_stage['Customer Address 2'],df_stage['Customer Address 3'],df_stage['Customer Address 4'],df_stage['State'],df_stage['Zip Code']), 256))

df_stage = df_stage.withColumn('PK_TEMPO', sha2(concat_ws('_', df_stage['DateKey'],df_stage['Promised Delivery Date'],df_stage['Actual Delivery Date'],df_stage['Invoice Date']), 256))

# criando o fato
ft_vendas = []

#criando as dimensões
DIM_CLIENTES = spark.sql("""
SELECT DISTINCT
PK_CLIENTE,
CustomerKey,
Customer,
`Customer Type`,
`Line Of Business`,
`Business Unit`,
`Business Family`,
`Regional Sales Mgr`,
Phone,
Division,
`Address Number`

FROM stage
""")]

DIM_LOCALIDADE = spark.sql("""
SELECT DISTINCT
PK_LOCAL,
`Address Number`,
`Customer Address 1`,
City,
Country,
State,
`Region Name`,
`Division Name`


FROM stage
""")

DIM_TEMPO = spark.sql("""
SELECT DISTINCT
PK_TEMPO,
DateKey,
`Promised Delivery Date`,
`Actual Delivery Date`,
`Invoice Date`

FROM stage
""")

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