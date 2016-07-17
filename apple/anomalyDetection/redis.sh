#!/bin/bash
source /etc/profile
path=$(cd $(dirname $0);pwd)
source ${path}/config

end=$1
livelen=${#live}

spark-submit --class Filter --master spark://10.254.171.85:7077 --total-executor-cores ${cores} --executor-memory ${mem} ${path}/anomaly.jar ${filter_out} ${end} ${partitions} ${ip_device_filter} ${ip_video_filter} ${idfa_video_filter}

redisDataDir=/data/datas/detection/redisdata
#删除重复的文件
rm -r /data/datas/detection/ip/${end}
rm -r /data/datas/detection/idfa/${end}
#清空redisdata文件夹
rm /data/datas/detection/redisdata/*
#清空tmp文件夹
rm /data/datas/detection/tmp/*

hadoop fs -get ${filter_out}/ip/${end} /data/datas/detection/ip/${end}
rm /data/datas/detection/ip/${end}/_SUCCESS
#key按hash分配到不同的文件中

for j in `ls -lrt /data/datas/detection/ip/${end} |grep -v total |awk '{print $9}'`
        do

ruby ${path}/redis_cluster_for_hadoop.rb ${redis_host} ${redis_port} /data/datas/detection/ip/${end}/${j} /data/datas/detection
#
for i in `ls -lrt /data/datas/detection/tmp |grep -v total |awk '{print $9}'`
     do
cat /data/datas/detection/tmp/${i} |awk '{print $1,length($1),$2,length($2)}' | awk '{print "*3\r\n$3\r\nset\r\n$" $2 "\r\n" $1 "\r\n$3\r\n999\r\n\r"}' > ${redisDataDir}/${i}
len=`expr length ${i}`

if [ ${len} -eq 19 ]; then
        h=${i:0:14}
        else
        h=${i:0:13}
fi

p=${i:0-4}
#
cat ${redisDataDir}/$i | /usr/local/bin/redis-cli -h ${h} -p ${p} -c --pipe
done
rm /data/datas/detection/redisdata/*
rm /data/datas/detection/tmp/*
sleep 5
done
#

hadoop fs -get ${filter_out}/idfa/${end} /data/datas/detection/idfa/${end}

rm /data/datas/detection/idfa/${end}/_SUCCESS
#key按hash分配到不同的文件中

for j in `ls -lrt /data/datas/detection/idfa/${end} |grep -v total |awk '{print $9}'`
        do

ruby ${path}/redis_cluster_for_hadoop.rb ${redis_host} ${redis_port} /data/datas/detection/idfa/${end}/${j} /data/datas/detection

#读取文件，向redis中写数据
#
for i in `ls -lrt /data/datas/detection/tmp |grep -v total |awk '{print $9}'`
     do
cat /data/datas/detection/tmp/${i} |awk '{print $1,length($1)}' | awk '{print "*4\r\n$4\r\nhset\r\n$" $2 "\r\n" $1 "\r\n$3\r\nnht\r\n$1\r\n1\r\n\r\n*3\r\n$6\r\nexpire\r\n$" $2 "\r\n" $1 "\r\n$"'${livelen}'"\r\n"'${live}'"\r\n\r"}' > ${redisDataDir}/${i}

len=`expr length ${i}`

if [ ${len} -eq 19 ]; then
        h=${i:0:14}
        else
        h=${i:0:13}
fi

p=${i:0-4}

cat ${redisDataDir}/$i | /usr/local/bin/redis-cli -h ${h} -p ${p} -c --pipe
done
rm /data/datas/detection/redisdata/*
rm /data/datas/detection/tmp/*
sleep 5
done
