#!/bin/bash
source /etc/profile
path=$(cd $(dirname $0);pwd)
source ${path}/config

spark-submit --class KeyWordsReport --master spark://10.254.171.85:7077 --total-executor-cores ${cores} --executor-memory ${mem} --jars ${depend} --driver-class-path ${driver} ${path}/keywords.jar $1
