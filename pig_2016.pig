A = LOAD 'logs_raw/2016' using PigStorage(',') AS (Timestamp:chararray,PCI:int,CellId:long,Latitude:float,Longitude:float,RSRP:float,SINR:float,RSRQ:chararray,PSC:int,RSCP:float,Eclo:float,Throughput:float,Temperature:chararray,waste1:int,waste2:int,waste3:int,name:chararray);

B = FOREACH A GENERATE REPLACE(Timestamp,' ',',') AS mytime_1,PCI,CellId,Latitude,Longitude,RSRP,SINR,RSRQ,PSC,RSCP,Eclo,Throughput,Temperature,REPLACE(name,'.csv','') as name_new;

C = FOREACH B GENERATE SUBSTRING(mytime_1,4,24) AS mytime_2,PCI,CellId,Latitude,Longitude,RSRP,SINR,RSRQ,PSC,RSCP,Eclo,Throughput,Temperature,name_new;

D = FOREACH C GENERATE REPLACE(mytime_2,'abr','apr') as mytime_3,PCI,CellId,Latitude,Longitude,RSRP,SINR,RSRQ,PSC,RSCP,Eclo,Throughput,Temperature,name_new;

E = FOREACH D GENERATE REPLACE(mytime_3,'ago','aug') as mytime_4,PCI,CellId,Latitude,Longitude,RSRP,SINR,RSRQ,PSC,RSCP,Eclo,Throughput,Temperature,name_new;

F = FOREACH E GENERATE REPLACE(mytime_4,'ene','jan') as mytime,PCI,CellId,Latitude,Longitude,RSRP,SINR,RSRQ,PSC,RSCP,Eclo,Throughput,Temperature,name_new;

G = FOREACH F GENERATE ToDate(mytime,'MMM,dd,HH:mm:ss,yyyy') AS newTime,PCI,CellId,Latitude,Longitude,RSRP,SINR,RSRQ,PSC,RSCP,Eclo,Throughput,Temperature,FLATTEN(STRSPLIT(name_new,'\\_')) AS (a:chararray,market_name:chararray,c:chararray,site_id:chararray,sa_name:chararray,date_name:chararray,time_site:chararray,imei:chararray);

H = FOREACH G GENERATE GetYear(newTime) AS year,GetMonth(newTime) AS month,GetDay(newTime) AS day,GetHour(newTime) AS hour,GetMinute(newTime) AS minute,GetSecond(newTime) AS second,PCI,CellId,Latitude,Longitude,RSRP,SINR,RSRQ,PSC,RSCP,Eclo,Throughput,Temperature,site_id,imei;

I = FOREACH H GENERATE CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT((chararray)year,'-'),(chararray)month),'-'),(chararray)day),' '),(chararray)hour),':'),(chararray)minute),':'),(chararray)second) AS TimeStamp,PCI,CellId,Latitude,Longitude,RSRP,SINR,REPLACE(RSRQ,'2.14748365E9','NULL') AS RSRQ_new,PSC,RSCP,Throughput,SUBSTRING(Temperature,0,4) AS Temp,site_id,imei;

STORE I INTO 'logs_final/2016';

