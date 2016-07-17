#!/bin/bash
source /etc/profile

path=$(cd $(dirname $0);pwd)
source ${path}/config

spark-submit --class Features --master spark://10.254.171.85:7077 --executor-memory 4g --total-executor-cores 12 ${path}/LRmodel.jar ${column_short_path} ${feature_index_path} $2 $3
wait
spark-submit --class Formatlog --master spark://10.254.171.85:7077 --executor-memory 5g --total-executor-cores 15 ${path}/LRmodel.jar ${column_short_path} ${feature_index_path} ${format_data_path} $2 $3

wait
spark-submit --class LR --master spark://10.254.171.85:7077 --executor-memory 5g --total-executor-cores 15 ${path}/LRmodel.jar ${format_data_path} ${training} ${test} ${save_model_path_pre}

wait
spark-submit --class ToRedis --master spark://10.254.171.85:7077 --executor-memory 1g --total-executor-cores 3 ${path}/LRmodel.jar ${save_model_path_pre} ${feature_index_path} ${host} ${port} $1 ${column_short_path}
