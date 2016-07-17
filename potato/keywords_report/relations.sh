#!/bin/bash
source /etc/profile
targetDay=$1
path=$(cd $(dirname $0);pwd)
source ${path}/config

spark-submit --class KeywordRelation --master spark://10.254.171.85:7077 --total-executor-cores ${cores} --executor-memory ${mem} --driver-class-path ${driver} ${path}/keywordRelactions.jar ${relation_keywords_file_path} ${targetDay} ${relation_out_path_pre}
