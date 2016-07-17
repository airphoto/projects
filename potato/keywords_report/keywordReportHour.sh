#!/bin/bash

source /etc/profile
if [ $# -eq 1 ]; then
        targetHour=$1
else
        targetHour=`date -d "-1 hours" +"%Y%m%d%H"`
fi
targetDate=${targetHour:0:8}
hour=${targetHour:0-2}
path=$(cd $(dirname $0);pwd)
source ${path}/config

spark-submit --class KeywordsReportHour --master spark://10.254.171.85:7077 --total-executor-cores ${cores} --executor-memory ${mem} --driver-class-path ${driver} ${path}/keywordsReportPerHour.jar ${targetDate} ${hour} ${report_hour_out_pre}
