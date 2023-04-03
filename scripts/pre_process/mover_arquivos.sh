#!/bin/bash

# Movendo pastas para o hdfs

DADOS=("CLIENTES" "DIVISAO" "ENDERECO" "REGIAO" "VENDAS")

echo "Movendo arquivos"

for i in "${DADOS[@]}"
do
	echo "$i"
    hdfs dfs -chmod 777 /datalake/raw/$i
    hdfs dfs -copyFromLocal $i.csv /datalake/raw/$i

done
