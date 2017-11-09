# Transferring log files after ETL using pig 
# Database and logfiles essentially have same data from tests but the log files have detailed information for each drive. 
# The log files are named with a standard convention and the information the name contains is not inside the file. Following code inserts the name of the file as a column.
ls *.csv|xargs -I% sed -i 's/$/,%/' %
# To be applied to a folder
# ETL process

A = LOAD 'logs_raw/2016' using PigStorage(',') AS (Timestamp:chararray,PCI:int,CellId:long,Latitude:float,Longitude:float,RSRP:float,SINR:float,RSRQ:chararray,PSC:int,RSCP:float,Eclo:float,Throughput:float,Temperature:chararray,waste1:int,waste2:int,waste3:int,name:chararray); 
# loading the files(could be from local or hdfs)
B = FOREACH A GENERATE REPLACE(Timestamp,' ',',') AS mytime_1,PCI,CellId,Latitude,Longitude,RSRP,SINR,RSRQ,PSC,RSCP,Eclo,Throughput,Temperature,REPLACE(name,'.csv','') as name_new;
# Clensing the timestamp column it is in the format fri oct 09,12:54:00,2017 we need it in ("YYYY-MM-DD HH:MM:SS.ff") as this is the hive accepted format and getting rid of extension from the file name we inserted using the code on line 4 
C = FOREACH B GENERATE SUBSTRING(mytime_1,4,24) AS mytime_2,PCI,CellId,Latitude,Longitude,RSRP,SINR,RSRQ,PSC,RSCP,Eclo,Throughput,Temperature,name_new;
# The part of the code gets rid of the jan, feb etc., part of the timestamp as it is redundant and incompatible with hive. It uses substring function which trims down a column to desired length with reference with indexes. Index starts with zero. 
D = FOREACH C GENERATE REPLACE(mytime_2,'abr','apr') as mytime_3,PCI,CellId,Latitude,Longitude,RSRP,SINR,RSRQ,PSC,RSCP,Eclo,Throughput,Temperature,name_new;
# The month part in timestamp is in spanish for a few records. This line transforms that spanish part into english
E = FOREACH D GENERATE REPLACE(mytime_3,'ago','aug') as mytime_4,PCI,CellId,Latitude,Longitude,RSRP,SINR,RSRQ,PSC,RSCP,Eclo,Throughput,Temperature,name_new;
# The month part in timestamp is in spanish for a few records. This line transforms that spanish part into english
F = FOREACH E GENERATE REPLACE(mytime_4,'ene','jan') as mytime,PCI,CellId,Latitude,Longitude,RSRP,SINR,RSRQ,PSC,RSCP,Eclo,Throughput,Temperature,name_new;
# The month part in timestamp is in spanish for a few records. This line transforms that spanish part into english
G = FOREACH F GENERATE ToDate(mytime,'MMM,dd,HH:mm:ss,yyyy') AS newTime,PCI,CellId,Latitude,Longitude,RSRP,SINR,RSRQ,PSC,RSCP,Eclo,Throughput,Temperature,FLATTEN(STRSPLIT(name_new,'\\_')) AS (a:chararray,market_name:chararray,c:chararray,site_id:chararray,sa_name:chararray,date_name:chararray,time_site:chararray,imei:chararray);
# Taking the file name column and splitting it into various columns(to be used for different purposes) with specific names and data types. 
H = FOREACH G GENERATE GetYear(newTime) AS year,GetMonth(newTime) AS month,GetDay(newTime) AS day,GetHour(newTime) AS hour,GetMinute(newTime) AS minute,GetSecond(newTime) AS second,PCI,CellId,Latitude,Longitude,RSRP,SINR,RSRQ,PSC,RSCP,Eclo,Throughput,Temperature,site_id,imei;
# GetMonth, GetDay are sql like functions which fetches hdfs compatable time
I = FOREACH H GENERATE CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT((chararray)year,'-'),(chararray)month),'-'),(chararray)day),' '),(chararray)hour),':'),(chararray)minute),':'),(chararray)second) AS TimeStamp,PCI,CellId,Latitude,Longitude,RSRP,SINR,REPLACE(RSRQ,'2.14748365E9','NULL') AS RSRQ_new,PSC,RSCP,Throughput,SUBSTRING(Temperature,0,4) AS Temp,site_id,imei;
# Concatinating the bits of time stamp we generated in previous step. Reason for generating the bits and concatinating in certain order is to make use of this column as a date value in hive tables instead of string as mathematical operations such as after 09/16/2017 cannot be applied is the date is in string type.
# Adios!
STORE I INTO 'logs_final/2016';
