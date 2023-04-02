#!/bin/bash

# Criação das pastas

DADOS=("CLIENTES" "DIVISAO" "ENDERECO" "REGIAO" "VENDAS")

echo "Criando pastas no HDFS ..."

for i in "${DADOS[@]}"
do
	echo "$i"
    cd ../../raw/
    hdfs dfs -mkdir /datalake/raw/$i

echo "Movendo arquvivos"

    hdfs dfs -chmod 777 /datalake/raw/$i
    hdfs dfs -copyFromLocal $i.csv /datalake/raw/$i

echo "Criando tabelas"

    beeline -u jdbc:hive2://localhost:10000 -f ../../scripts/hql/create_table_$i.hql 
done
