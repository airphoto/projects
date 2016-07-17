#!/bin/bash
source /etc/profile
path=$(cd $(dirname $0);pwd)
source ${path}/config

java -cp ${path}/keywordMQ.jar KeywordRecv ${host} ${username} ${pwd} ${path}/keywordrequest.sh
java -cp ${path}/keyqueue.jar Recv ${host} ${username} ${pwd} ${path}/keywords.sh
