#!/bin/bash
source /etc/profile
path=$(cd $(dirname $0);pwd)
source ${path}/config

time=`date -d "-1 hours" +"%Y%m%d%H"`

spark-submit --class RequestData --master spark://10.254.171.85:7077 --total-executor-cores ${cores} --executor-memory ${mem} --jars ${depend} --driver-class-path ${driver} ${path}/keywordsReportPerHour.jar ${time} $1 ${path}
