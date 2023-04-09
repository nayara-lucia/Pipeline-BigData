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
Para isso, utilizando linguagem SQL dentro do Spark, foi realizado diversos joins para que fosse possível chegar ao resultado esperado, além da necessidade de tratar os dados de acordo com a necessidade do cliente, como por exemplo preencher campos string vazios com "Não informado", etc.
<br></br>
Após o tratamento de dados e a criação do nosso modelo estrela, com as tabelas tratadas criamos nossa estrutura no PowerBI para que a área de Business Intelligence consiga realizar consultas e análises para tomada de decisões.
<br></br>

![image](https://user-images.githubusercontent.com/126920974/230743241-db2c1ee3-cc80-432c-9803-c641f210c64f.png)
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
<li> Tabela stage e desnormatização de dados </li>
<li> PowerBI para apresentação das informações </li>

