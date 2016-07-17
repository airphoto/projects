#!/bin/bash
source /etc/profile
if [ $# -eq 1 ]; then
	targetDate=$1
else
	targetDate=`date -d "-1 days" +"%Y%m%d"`
fi

dayForMysql="${targetDate:0:4}-${targetDate:0-4:2}-${targetDate:0-2}"
path=$(cd $(dirname $0);pwd)
source ${path}/config

hive -e "add jar ${path}/device.jar;drop temporary function checkos;create temporary function checkos as 'com.hq.traffic.udf.Os';insert overwrite table ${hive_database}.hq_traffic_report partition (d=${targetDate}) select '"${dayForMysql}"','youku',if(tagid is not null,tagid,'-'),channel,if(device_type=0,'Mobile',(if(device_type=1,'PC','Tablet'))),checkos(os),width,height,count(*) from ${hive_database}.bid_request where d=${targetDate} group by '"${dayForMysql}"','youku',if(tagid is not null,tagid,'-'),channel,if(device_type=0,'Mobile',(if(device_type=1,'PC','Tablet'))),checkos(os),width,height"

wait
mysql -u ${username} --password=${psw} -h ${host} -P ${port} -e "delete from hq_traffic_report where day='${dayForMysql}'" ${database}

wait
sqoop export --connect jdbc:mysql://${host}:${port}/${database}?tinyInt1isBit=false --username ${username} --password ${psw} --export-dir /user/hive/warehouse/${hive_database}.db/hq_traffic_report/d="${targetDate}" --table hq_traffic_report --columns "day,platform,placement,channel,device,system,width,height,count" --fields-terminated-by '\t' --outdir=${path}/src --input-null-string '\\N' --input-null-non-string '\\N'
