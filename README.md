# 基于论坛的apache common日志分析项目
## 项目描述
通过对技术论坛的apache common日志进行分析，开发该系统的目的是分了获取一些业务相关的指标，这些指标在第三方工具中无法获得的；计算论坛关键指标，供运营者决策。
## 项目设计
1.把日志数据上传到HDFS中进行处理				
- 如果是日志服务器数据较小、压力较小，可以直接使用shell命令把数据上传到HDFS中；			
- 如果是日志服务器数据较大、压力较答，使用NFS在另一台服务器上上传数据；
- 如果日志服务器非常多、数据量大，使用flume进行数据处理；

2.使用MapReduce对HDFS中的原始数据进行清洗；

3.使用Hive对清洗后的数据进行统计分析；

4.使用Sqoop把Hive产生的统计结果导出到mysql中；

5.如果用户需要查看详细数据的话，可以使用HBase进行展现；

![日志报告分析](https://i.imgur.com/q2Bl7G6.png)

## 开发步骤：

- <a href="#1">使用flume把日志数据导入</a>
- <a href="#2">MapReduce程序计算KPI</a>
- <a href="#3">HBASE详单查询</a>
- <a href="#4">HIVE数据仓库多维分析</a>
- <a href="#5">sqoop导出分析数据</a>
- <a href="#6">编写定时任务调度脚本</a>

### <a name="1">使用flume把日志数据导入到hdfs中<后续补充></a>
>flume(源是文件夹，目的是hdfs和hbase，管道是文件)

### <a name="2">对数据进行清洗</a>
>mapreduce

源码 [bbsCleaner.java](/src/com/elon33/bbs/bbsCleaner.java "点击此处查看源码")

数据清洗结果
>hadoop fs -cat /user/elon/bbs_cleaned/2013_05_30/part-r-00000
	
![](https://i.imgur.com/6PPetpR.png)
### <a name="3">明细日志使用hbase存储，能够利用ip、时间查询</a>
>设计表、预分区

源码 [bbsHBase.java](/src/com/elon33/bbs/bbsHBase.java "点击此处查看源码")

HBase中 bbs_log表存储结果
![](https://i.imgur.com/vOGjQt7.png)

### <a name="4">使用hive进行数据的多维分析</a>
>hive(表、视图)、自定义函数

	## 存放数据的主分区表
	hive -e "ALTER TABLE bbs ADD PARTITION(logdate='2013_05_30') LOCATION 'hdfs://hadoop:9000/user/elon/bbs_cleaned/2013_05_30';"
	
	## create hive table everyday
	## 统计单日PV数
	hive -e "CREATE TABLE bbs_pv_2013_05_30 AS SELECT COUNT(1) AS PV FROM bbs WHERE logdate='2013_05_30';"
	
	## 统计单日注册数
	hive -e "CREATE TABLE bbs_reguser_2013_05_30 AS SELECT COUNT(1) AS REGUSER FROM bbs WHERE logdate='2013_05_30' AND INSTR(url,'member.php?mod=register')>0;"

	## 统计单日访问IP用户数
	hive -e "CREATE TABLE bbs_ip_2013_05_30 AS SELECT COUNT(DISTINCT ip) AS IP FROM bbs WHERE logdate='2013_05_30';"

	## 统计单日跳出数
	hive -e "CREATE TABLE bbs_jumper_2013_05_30 AS SELECT COUNT(1) AS jumper FROM (SELECT COUNT(ip) AS times FROM bbs WHERE logdate='2013_05_30' GROUP BY ip HAVING times=1) e;"

	## 以上四个结果汇总到一张表统计
	hive -e "CREATE TABLE bbs_2013_05_30 AS SELECT '2013_05_30', a.pv, b.reguser, c.ip, d.jumper FROM bbs_pv_2013_05_30 a JOIN bbs_reguser_2013_05_30 b ON 1=1 JOIN bbs_ip_2013_05_30 c ON 1=1 JOIN bbs_jumper_2013_05_30 d ON 1=1;"

汇总表结果
![](https://i.imgur.com/u5Kbhiq.png)

### <a name="5">把hive分析结果使用sqoop导出到mysql中</a>
>sqoop、MySQL

创建bbs表
![](https://i.imgur.com/s8mmGHM.png)

	sqoop export --connect jdbc:mysql://hadoop:3306/bbs --username root --password 123456 --table bbs_logs --fields-terminated-by '\001' --export-dir 'hdfs://hadoop:9000/user/hive/warehouse/bbs_2013_05_30'
	
![](https://i.imgur.com/9sF6ZQm.png)

当最终分析的论坛指标数据导出到MySQL中时，之前那些临时表就可以删除了。在下面的自动调度中，实现临时表删除。

### <a name="6">最后，使用linux的crontab做自动调度</a>

要想通过脚本实现每天自动调度进行日志分析，就必须用到shell脚本，将命令都封装在shell脚本中，通过每天日期的迭代和定时任务设置，实现自动调度分析日志。

1.在 [crontab-e](/crontab-e "点击此处查看源码") 中实现定时任务设置。

2.在 [ bbs_daily.sh](/bbs_daily.sh "点击此处查看源码") 脚本中每日调度一次`bbs_common.sh`脚本，传入每日的日期参数，实现任务调度。

3.在 [bbs_common.sh](/bbs_common.sh "点击此处查看源码") 调用命令脚本实现日志分析过程，得到分析指标结果。
