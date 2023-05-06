 ### Desafio_Final_Big_Data
 
<h1 align="center"> Pipeline de Dados </h1>
<h4 align="center">Projeto final para o programa Jovens Profissionais BI/BA realizado pela Minsait Indra, com intuito de apresentar as habilidades adquiridas durante a jornada de aprendizado no programa.</h4>
<h4 tabindex="-1" dir="auto">📌 Principais tecnologias utilizadas</h4>

![MySQL](https://img.shields.io/badge/mysql-%2300f.svg?style=for-the-badge&logo=mysql&logoColor=white) ![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54) ![Shell Script](https://img.shields.io/badge/shell_script-%23121011.svg?style=for-the-badge&logo=gnu-bash&logoColor=white) ![Gitpod](https://img.shields.io/badge/gitpod-f06611.svg?style=for-the-badge&logo=gitpod&logoColor=white) ![Git](https://img.shields.io/badge/GIT-E44C30?style=for-the-badge&logo=git&logoColor=white) ![Jupyter Notebook](https://img.shields.io/badge/jupyter-%23FA0F00.svg?style=for-the-badge&logo=jupyter&logoColor=white) ![Linux](https://img.shields.io/badge/Linux-FCC624?style=for-the-badge&logo=linux&logoColor=black) ![Apache](https://img.shields.io/badge/apache-%23D42029.svg?style=for-the-badge&logo=apache&logoColor=white)


<h4 tabindex="-1" dir="auto"></a> 📍 Descrição do projeto</h4>

Nesse projeto foi apresentado um desenvolvimento de um Pipeline de ingestão de dados utilizando Hive na arquitetura do Apache Hadoop juntamente com o processamento e tratamento dos dados com Spark. Projeto feito para ser usado pela área de Business Intelligence utilizar para análises no PowerBI e realizar a tomada de decisões


<h4 tabindex="-1" dir="auto">⚒️ Construção do Projeto ⚒️</h4>
Para a movimentação e criação de pastas dentro do HDFS e também da criação das tabelas externas no Hive foi utilizado ShellScript para a automação do processo.
<br></br>
Foram utilizadas tabelas de vendas, clientes, endereço, região, divisão com esses dados foi necessário realizar a desnormatização das tabelas e transforma-las em um modelo dimensional de formato estrela.
Para isso, utilizando linguagem SQL dentro do Spark, foi realizado diversos joins para que fosse possível chegar ao resultado esperado, além da necessidade de tratar os dados de acordo com a necessidade do cliente, como por exemplo preencher campos string vazios com "Não informado", etc. Para nossa tabela stage, criamos as devidas DW keys, e em seguida realizando a criação e identificação da nossa Fato (Vendas) e Dimensões (Localidade, Tempo e Clientes).
<br></br>
Após o tratamento de dados e a criação do nosso modelo estrela, com as tabelas tratadas, já na fase Gold do nosso DataLake, criamos nossa estrutura no PowerBI para que a área de Business Intelligence consiga realizar consultas e análises para tomada de decisões.
<br></br>

![image](https://user-images.githubusercontent.com/126920974/233228015-2469df4e-c3fa-4f63-b367-2d9ba2d08c2c.png)
![image](https://user-images.githubusercontent.com/126920974/236626739-c72be5be-c0e7-4bee-905c-d1d8dcc5008a.png)
<br></br>
<h4 tabindex="-1" dir="auto">📚 Principais conceitos aprendidos</h4>
<li> 5 Vs do Big Data e sua importância para ánalise de dados </li>
<li> Modelo estrela x Snowflake </li>
<li> Conceitos de Docker e Manipulação dos contêineres </li>
<li> Hadoop e sua arquitetura </li>
<li> Namenode x Datanode e sua relação com o HDFS </li>
<li> Comandos Linux </li>
<li> Conceitos de Datalake </li>
<li> ETL x ELT </li>
<li> Tabelas internas e externas no Hive </li>
<li> Processamento de dados no Spark utilizando Pyspark e SQL </li>
<li> Tabela stage e desnormatização de dados </li>
<li> PowerBI para apresentação das informações </li>
<br></br>
📌 ESCOPO DO DESAFIO

Neste desafio serão feitas as ingestões dos dados que estão na pasta /raw onde vamos ter alguns arquivos .csv de um banco relacional de vendas.

 - VENDAS.CSV
 - CLIENTES.CSV
 - ENDERECO.CSV
 - REGIAO.CSV
 - DIVISAO.CSV

Seu trabalho como engenheiro de dados/arquiteto de BI é prover dados em uma pasta desafio_curso/gold em .csv para ser consumido por um relatório em PowerBI que deverá ser construído dentro da pasta 'app' (já tem o template).

📑 ETAPAS

Etapa 1 - Enviar os arquivos para o HDFS
    - nesta etapa lembre de criar um shell script para fazer o trabalho repetitivo (não é obrigatório)

Etapa 2 - Criar o banco DEASFIO_CURSO e dentro tabelas no Hive usando o HQL e executando um script shell dentro do hive server na pasta scripts/pre_process.

    - DESAFIO_CURSO (nome do banco)
        - TBL_VENDAS
        - TBL_CLIENTES
        - TBL_ENDERECO
        - TBL_REGIAO
        - TBL_DIVISAO

Etapa 3 - Processar os dados no Spark Efetuando suas devidas transformações criando os arquivos com a modelagem de BI.
OBS. o desenvolvimento pode ser feito no jupyter porem no final o codigo deve estar no arquivo desafio_curso/scripts/process/process.py

Etapa 4 - Gravar as informações em tabelas dimensionais em formato cvs delimitado por ';'.

        - FT_VENDAS
        - DIM_CLIENTES
        - DIM_TEMPO
        - DIM_LOCALIDADE

Etapa 5 - Exportar os dados para a pasta desafio_curso/gold

Etapa 6 - Criar e editar o PowerBI com os dados que você trabalhou.

No PowerBI criar gráficos de vendas.



REGRAS
Campos strings vazios deverão ser preenchidos com 'Não informado'.
Campos decimais ou inteiros nulos ou vazios, deversão ser preenchidos por 0.
Atentem-se a modelagem de dados da tabela FATO e Dimensão.
Na tabela FATO, pelo menos a métrica <b>valor de venda</b> é um requisito obrigatório.
Nas dimensões deverá conter valores únicos, não deverá conter valores repetidos.

