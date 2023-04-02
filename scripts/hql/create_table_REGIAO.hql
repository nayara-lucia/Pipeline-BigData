CREATE EXTERNAL TABLE IF NOT EXISTS desafio_curso.TBL_REGIAO ( 
        Region Code string,
        Region Name string
    )
COMMENT 'Tabela de Regiao'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/REGIAO/'
TBLPROPERTIES ("skip.header.line.count"="1");