CREATE EXTERNAL TABLE IF NOT EXISTS desafio_curso.TBL_ENDERECO ( 
        `Address Number` string,
        City string,
        Country string,
        `Customer Address 1` string,
        `Customer Address 2` string,
        `Customer Address 3` string,
        `Customer Address 4` string,
        State string,
        `Zip Code` string

    )
COMMENT 'Tabela de Enderecos'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/ENDERECO/'
TBLPROPERTIES ("skip.header.line.count"="1");