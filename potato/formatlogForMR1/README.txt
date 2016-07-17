--流程说明

1、原始数据通过fluentd传输到HDFS
路径：
/logs/source/tracking/%Y%m%d/${hostname}.%Y%m%d%H
/logs/source/bidder/%Y%m%d/${hostname}.%Y%m%d%H

每个小时生成一个gzip压缩文件

2、每个小时的第5分钟启动上个小时的原始日志清理、报表计算任务（目前为root账户的crontab任务）

3、计算完的报表生成本地文件存在hive中间表中，hq_report,hq_geo_report,hq_channel_report
4、sqoop执行数据由hive到mysql的转换

--文件说明

1、formatlogForMR1  清洗原始日志的java源代码
2、logformat.jar  清洗日志的文件

3、formatTrackingLog.sh        日志清洗、报表计算、报表导入MySQL流程控制脚本

4、config                      报表要插入的MySQL数据库配置信息

5、hive-table			Hive表建表语句

6、report_results目录为每个小时生成的报表数据


--Hive表说明
清洗完的数据存在formatlog库中的hq_tracking_format_hour，hq_report,hq_geo_report,hq_channel_report表中，

建表语句参考hive-table