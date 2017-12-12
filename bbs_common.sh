#!/bin/sh
	
#get yesterday format string
#yesterday=`date --date='1 days ago' +%Y_%m_%d`
yesterday=$1

#upload logs to hdfs
hadoop fs -put /apache_logs/access_${yesterday}.log  /bbs_logs

#cleaning data
hadoop jar /apache_logs/cleaned.jar  /bbs_logs/access_${yesterday}.log  /bbs_cleaned/${yesterday}  1>/dev/null


#alter hive table and then add partition to existed table
hive -e "ALTER TABLE bbs ADD PARTITION(logdate='${yesterday}') LOCATION '/bbs_cleaned/${yesterday}';"

#create hive table everyday
hive -e "CREATE TABLE bbs_pv_${yesterday} AS SELECT COUNT(1) AS PV FROM bbs WHERE logdate='${yesterday}';"
hive -e "CREATE TABLE bbs_reguser_${yesterday} AS SELECT COUNT(1) AS REGUSER FROM bbs WHERE logdate='${yesterday}' AND INSTR(url,'member.php?mod=register')>0;"
hive -e "CREATE TABLE bbs_ip_${yesterday} AS SELECT COUNT(DISTINCT ip) AS IP FROM bbs WHERE logdate='${yesterday}';"
hive -e "CREATE TABLE bbs_jumper_${yesterday} AS SELECT COUNT(1) AS jumper FROM (SELECT COUNT(ip) AS times FROM bbs WHERE logdate='${yesterday}' GROUP BY ip HAVING times=1) e;"
hive -e "CREATE TABLE bbs_${yesterday} AS SELECT '${yesterday}', a.pv, b.reguser, c.ip, d.jumper FROM bbs_pv_${yesterday} a JOIN bbs_reguser_${yesterday} b ON 1=1 JOIN bbs_ip_${yesterday} c ON 1=1 JOIN bbs_jumper_${yesterday} d ON 1=1;"

#delete hive tables
hive -e "drop table bbs_pv_${yesterday};"
hive -e "drop table bbs_reguser_${yesterday};"
hive -e "drop table bbs_ip_${yesterday};"
hive -e "drop table bbs_jumper_${yesterday};"


#sqoop export to mysql
sqoop export --connect jdbc:mysql://hadoop0:3306/bbs --username root --password admin --table bbs_logs --fields-terminated-by '\001' --export-dir '/user/hive/warehouse/bbs_${yesterday}'

#delete hive tables
hive -e "drop table bbs_${yesterday};"