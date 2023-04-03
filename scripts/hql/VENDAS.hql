CREATE EXTERNAL TABLE IF NOT EXISTS desafio_curso.TBL_VENDAS ( 
        `Actual Delivery Date` string,
        `CustomerKey` string,
        `DateKey` string,
        `Discount Amount`	string,
        `Invoice Date` string,	
        `Invoice Number` string,
        `Item Class` string,
        `Item Number` string,
        `Item` string,
        `Line Number`	string,
        `List Price` string,
        `Order Number` string,	
        `Promised Delivery Date` string,	
        `Sales Amount` string,	
        `Sales Amount Based on List Price` string,	
        `Sales Cost Amount` string,
        `Sales Margin Amount` string,	
        `Sales Price` string,	
        `Sales Quantity` string,	
        `Sales Rep` string,	
        `U/M` string


    )
COMMENT 'Tabela de Vendas'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/VENDAS/'
TBLPROPERTIES ("skip.header.line.count"="1");