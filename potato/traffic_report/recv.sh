#!/bin/bash
source /etc/profile
path=$(cd $(dirname $0);pwd)
source ${path}/config
chmod +x ${path}/traffic.sh

java -cp ${path}/recvqueue.jar Recv ${host} ${username} ${pwd} ${path}/traffic.sh
