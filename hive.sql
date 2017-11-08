set hive.cli.print.header=true;
set hive.resultset.use.unique.column.names=false;

CREATE EXTERNAL TABLE IF NOT EXISTS logs_2016 (date timestamp, pci int,
CellId String, latitude float, longitude float, RSRP float, SINR float, RSRQ float,PSC int, RSCP float, Throughput float, Temperature float, site_id string, imei bigint)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

load data inpath 'logs_final/2016' overwrite into table logs_2016;

CREATE EXTERNAL TABLE IF NOT EXISTS logs_2017 (date timestamp, pci int,
CellId String, latitude float, longitude float, RSRP float, SINR float, RSRQ float,PSC int, RSCP float, Throughput float, Temperature float, site_id string, imei bigint)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

load data inpath 'logs_final/2017' overwrite into table logs_2017;
