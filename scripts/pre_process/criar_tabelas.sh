#!/bin/bash

# Criação das tabelas

DADOS=("CLIENTES" "DIVISAO" "ENDERECO" "REGIAO" "VENDAS")

echo "Criando Tabela"

for i in "${DADOS[@]}"
do
    echo "$i"
    beeline -u jdbc:hive2://localhost:10000 -f /input/scripts/hql/$i.hql 
done
