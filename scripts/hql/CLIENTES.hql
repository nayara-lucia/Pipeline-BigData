CREATE EXTERNAL TABLE IF NOT EXISTS desafio_curso.TBL_CLIENTES ( 
        `Address Number` string,
        `Business Family` string,
        `Business Unit` string,
        Customer string,
        CustomerKey string,
        `Customer Type` string,
        Division string,
        `Line of Business` string,
        Phone string,
        `Region Code` string,
        `Regional Sales Mgr` string,
        `Search Type` string
    )
COMMENT 'Tabela de Clientes'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/CLIENTES/'
TBLPROPERTIES ("skip.header.line.count"="1");