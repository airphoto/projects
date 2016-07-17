source /etc/profile
targetDay=$1
path=$(cd $(dirname $0);pwd)
source ${path}/config

spark-submit --class ChannelPv --master spark://10.254.171.85:7077 --total-executor-cores ${cores} --executor-memory ${mem} --driver-class-path ${driver} ${path}/keywordpv.jar ${channel_keywords_file_path} ${targetDay} ${channel_out_pre}
wait
spark-submit --class KeywordPv --master spark://10.254.171.85:7077 --total-executor-cores ${cores} --executor-memory ${mem} --driver-class-path ${driver} ${path}/keywordpv.jar ${keywords_pv_file_path} ${targetDay} ${keywords_out_pre}
