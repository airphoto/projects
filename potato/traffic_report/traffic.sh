#!/bin/bash
source /etc/profile
path=$(cd $(dirname $0);pwd)
source ${path}/config
echo $1
spark-submit --class Traffic --master spark://10.136.49.179:7077 --total-executor-cores ${cores} --executor-memory ${mem} --driver-class-path ${driver} ${path}/traffic_report.jar $1
