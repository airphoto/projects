#!/bin/sh
source /etc/profile
path=$(cd $(dirname $0);pwd)
source ${path}/config

spark-submit --class LogStreaming --master spark://10.254.171.85:7077 --total-executor-cores ${cores} --executor-memory ${mem} --supervise --jars ${dependents} --driver-class-path ${driver} ${path}/logstreaming.jar ${brokers} ${group} ${topics} ${threads} ${secondToCount} ${table}
