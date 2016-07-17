#!/bin/bash
source /etc/profile
path=$(cd $(dirname $0);pwd)
source ${path}/config

if [ $# -eq 2 ]; then
   begain=$1
   end=$2
   else
   begain=`date -d "-${days_to} days" +"%Y%m%d"`
   end=`date -d "-3 days" +"%Y%m%d"`
fi

spark-submit --class Anomaly --master spark://10.254.171.85:7077 --total-executor-cores ${cores} --executor-memory ${mem} ${path}/anomaly.jar ${begain} ${end} ${out} ${part} ${database}
wait
sh ${path}/redis.sh ${end}
