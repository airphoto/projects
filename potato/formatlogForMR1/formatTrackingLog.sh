#!/bin/bash
# 5 * * * * /data/work/hq/HqLogFormat/formatTrackingLog.sh >/data/work/hq/HqLogFormat/formatTrackingLog.log 2>&1
source /etc/profile
if [ $# -eq 1 ]; then
	targetHour=$1
else
	targetHour=`date -d "-1 hours" +"%Y%m%d%H"`
fi
targetDate=${targetHour:0:8}
hour_pre=${targetHour:0-2}
hour=$((${hour_pre:0:1}*10+${hour_pre:0-1}))
dayForMysql="${targetDate:0:4}-${targetDate:0-4:2}-${targetDate:0-2}"
path=$(cd $(dirname $0);pwd)
source ${path}/config

# format tracking hour log
outputpath="${hadoop_tracking_out_path}/${targetDate}/${hour}"
# hadoop job
hadoop jar ${path}/logformat.jar com.hq.hadoop.format.FormatTrackingLog tracking_log_format_${targetHour} /logs/source/tracking/${targetDate}/*.${targetHour}.gz ${outputpath}
# add partition hq_tracking_format_hour

#wait
hive -e "use ${test};alter table hq_tracking_format_hour drop if exists partition (day=${targetDate},hour=${hour});alter table hq_tracking_format_hour add partition(day=${targetDate},hour=${hour}) location '${outputpath}'"

wait
hive -e "set mapred.job.name=[HaoQu][DSP][Report][${targetHour}];insert overwrite table ${test}.hq_all_report partition (d=${targetDate},h=${hour}) select * from (select hqclientid,hqgroupid,hqadid,hqcreativeid,tagid,sum(if(hqevent=1,1,0)) as impression,sum(if(hqevent=2,1,0)) as click,(sum(if(hqevent=if(hqsource='baidu_bc' or hqsource='tencent',1,3),if(hqprice is not null,hqprice,0),0))/1000) as cost,sum(if(hqevent=4,1,0)) as download,sum(if(hqevent=5,1,0)) as follow,sum(if(hqevent=6,1,0)) as install,sum(if(hqevent=7,1,0)) as active,hour,'"${dayForMysql}"',if(hqsource='bc','baidu_bc',hqsource),channelid,hqplacementid from ${test}.hq_tracking_format_hour where day=${targetDate} and hour=${hour} group by hqclientid,hqgroupid,hqadid,hqcreativeid,tagid,hour,if(hqsource='bc','baidu_bc',hqsource),channelid,hqplacementid) as tmp where tmp.impression>0 and tmp.cost>0"

wait
hive -e "set mapred.job.name=[HaoQu][DSP][Report][${targetHour}];insert overwrite table ${test}.hq_report partition (d=${targetDate},h=${hour}) select * from (select hqclientid,hqgroupid,hqadid,hqcreativeid,tagid,sum(if(hqevent=1,1,0)) as impression,sum(if(hqevent=2,1,0)) as click,(sum(if(hqevent=if(hqsource='baidu_bc' or hqsource='tencent',1,3),if(hqprice is not null,hqprice,0),0))/1000) as cost,sum(if(hqevent=4,1,0)) as download,sum(if(hqevent=5,1,0)) as follow,sum(if(hqevent=6,1,0)) as install,sum(if(hqevent=7,1,0)) as active,hour,'"${dayForMysql}"',if(hqsource='bc','baidu_bc',hqsource),hqplacementid from ${test}.hq_tracking_format_hour where day=${targetDate} and hour=${hour} group by hqclientid,hqgroupid,hqadid,hqcreativeid,tagid,hour,if(hqsource='bc','baidu_bc',hqsource),hqplacementid) as tmp where tmp.impression>0 and tmp.cost>0"
wait
hive -e "set mapred.job.name=[channel report][${targetHour}];insert overwrite table ${test}.hq_channel_report partition (d=${targetDate},h=${hour}) select * from (select hqclientid,hqgroupid,hqadid,channelid,sum(if(hqevent=1,1,0)) as impression,sum(if(hqevent=2,1,0)) as click,(sum(if(hqevent=if(hqsource='baidu_bc' or hqsource='tencent',1,3),if(hqprice is not null,hqprice,0),0))/1000) as cost,sum(if(hqevent=4,1,0)) as download,sum(if(hqevent=5,1,0)) as follow,sum(if(hqevent=6,1,0)) as install,sum(if(hqevent=7,1,0)) as active,hour,'"${dayForMysql}"',if(hqsource='bc','baidu_bc',hqsource) from ${test}.hq_tracking_format_hour where day=${targetDate} and hour=${hour} group by hqclientid,hqgroupid,hqadid,channelid,hour,if(hqsource='bc','baidu_bc',hqsource)) as tmp where tmp.impression>0 and tmp.cost>0"
wait
hive -e "set mapred.job.name=[geo report][${targetHour}];insert overwrite table ${test}.hq_geo_report partition (d=${targetDate},h=${hour}) select * from (select hqclientid,hqgroupid,hqadid,hqcity,sum(if(hqevent=1,1,0)) as impression,sum(if(hqevent=2,1,0)) as click,(sum(if(hqevent=if(hqsource='baidu_bc' or hqsource='tencent',1,3),if(hqprice is not null,hqprice,0),0))/1000) as cost,sum(if(hqevent=4,1,0)) as download,sum(if(hqevent=5,1,0)) as follow,sum(if(hqevent=6,1,0)) as install,sum(if(hqevent=7,1,0)) as active,hour,'"${dayForMysql}"',if(hqsource='bc','baidu_bc',hqsource) from ${test}.hq_tracking_format_hour where day=${targetDate} and hour=${hour} group by hqclientid,hqgroupid,hqadid,hqcity,hour,if(hqsource='bc','baidu_bc',hqsource)) as tmp where tmp.impression>0 and tmp.cost>0"

wait
mysql -u ${username} --password=${psw} -h ${host} -P ${port} -e "delete from hq_all_report where day='${dayForMysql}' and hour=${hour}" ${database}
wait
mysql -u ${username} --password=${psw} -h ${host} -P ${port} -e "delete from hq_report where day='${dayForMysql}' and hour=${hour}" ${database}
wait
mysql -u ${username} --password=${psw} -h ${host} -P ${port} -e "delete from hq_geo_report where day='${dayForMysql}' and hour=${hour}" ${database}
wait
mysql -u ${username} --password=${psw} -h ${host} -P ${port} -e "delete from hq_channel_report where day='${dayForMysql}' and hour=${hour}" ${database}

wait
sqoop export --connect jdbc:mysql://${host}:${port}/${database}?tinyInt1isBit=false --username ${username} --password ${psw} --export-dir /user/hive/warehouse/${test}.db/hq_all_report/d="${targetDate}"/h="$((hour))" --table hq_all_report --columns "client_id,group_id,ad_id,creative_id,tag_id,impression,click,cost,download,follow,install,active,hour,day,platform,channel_id,placementid" --fields-terminated-by '\t' --outdir=${path}/src --input-null-string '\\N' --input-null-non-string '\\N'

wait
sqoop export --connect jdbc:mysql://${host}:${port}/${database}?tinyInt1isBit=false --username ${username} --password ${psw} --export-dir /user/hive/warehouse/${test}.db/hq_report/d="${targetDate}"/h="$((hour))" --table hq_report --columns "client_id,group_id,ad_id,creative_id,tag_id,impression,click,cost,download,follow,install,active,hour,day,platform,placementid" --fields-terminated-by '\t' --outdir=${path}/src --input-null-string '\\N' --input-null-non-string '\\N'
wait
sqoop export --connect jdbc:mysql://${host}:${port}/${database}?tinyInt1isBit=false --username ${username} --password ${psw} --export-dir /user/hive/warehouse/${test}.db/hq_channel_report/d="${targetDate}"/h="$((hour))" --table hq_channel_report --columns "client_id,group_id,ad_id,channel_id,impression,click,cost,download,follow,install,active,hour,day,platform" --fields-terminated-by '\t' --outdir=${path}/src --input-null-string '\\N' --input-null-non-string '\\N'
wait
sqoop export --connect jdbc:mysql://${host}:${port}/${database}?tinyInt1isBit=false --username ${username} --password ${psw} --export-dir /user/hive/warehouse/${test}.db/hq_geo_report/d="${targetDate}"/h="$((hour))" --table hq_geo_report --columns "client_id,group_id,ad_id,city_code,impression,click,cost,download,follow,install,active,hour,day,platform" --fields-terminated-by '\t' --outdir=${path}/src --input-null-string '\\N' --input-null-non-string '\\N'


email=huasong.li@hogic.cn,yuchao.ma@hogic.cn
formatMessage(){
        msg="formatlog %s %s!"
        result=`printf "${msg}" "$1" "$2"`
        echo ${result}
}
formatContent(){
        echo "<html>"
        echo "<head>"
        echo "</head>"
        echo " <body>"
        echo " <font face=\"Microsoft YaHei\">"
        echo "<h2>formatlog日志 $1</h2>"
        cat $2 | while read line
        do
            echo $line "</br>"
        done
        echo "</body>"
        echo "</html>"
}
content=`formatMessage "${targetHour}" "fail"`
mailhtml=`formatContent "${targetHour}" "${path}/*.log"`
existsFile=`cat ${path}/*.log | grep 'matches 0 files' | wc -l`

success=`cat ${path}/*.log | grep 'completed successfully' | wc -l`
oks=`cat ${path}/*.log | grep OK | wc -l`

(([ ${success} != 5 ]||[ ${oks} != 11 ])&&[ ${existsFile} = 0 ]) && echo ${mailhtml} | formail -I "MIME-Version:1.0" -I "Content-type:text/html;charset=utf8" -I "Subject:${content}" | /usr/sbin/sendmail -oi ${email}
