### flume for apache doris sink 基本介绍

#### flume基于文件信息采集，并通过stream load导入模式，将采集数据，单笔或者攒批 导入doris集群，可适用于用户行为日志，应用错误日志等日志采集场景, 并通过apache doris构建对应场景的olap数据模型，赋能业务.

* 支持多fe负载
* 支持指定库，表，字段
* 支持json格式导入
* 支持单位时间内微批，攒批


#### flume doris sink基本配置参考

* taildir source 支持单文件，文件组监控
* file channel 支持故障续传

```
a1.sources = r1
a1.channels = c1
a1.sinks = k1

a1.sources.r1.type = taildir
a1.sources.r1.channels = c1
a1.sources.r1.positionFile = /data/flume/taildir_position.json
a1.sources.r1.filegroups = f1 f2
a1.sources.r1.filegroups.f1 = /data/flume/test.txt
a1.sources.r1.filegroups.f2 = /data/flume/test2/.*
a1.sources.r1.batchSize = 100
a1.sources.r1.fileHeader = true

a1.sinks.k1.type = com.darjuan.flume.doris.sinks.BatchSink
a1.sinks.k1.hosts = 172.20.1.90,172.20.1.91
a1.sinks.k1.port = 8030
a1.sinks.k1.user = root
a1.sinks.k1.password =
a1.sinks.k1.database = db_log
a1.sinks.k1.table = error_log
a1.sinks.k1.mergeType = APPEND
a1.sinks.k1.separator =$$$$    
a1.sinks.k1.columns = create_time,log_level,class, method, msg, exception, log_date
a1.sinks.k1.format =
a1.sinks.k1.jsonPaths =
a1.sinks.k1.where =
a1.sinks.k1.batchSize = 50
a1.sinks.k1.uniqueEvent = 1
a1.sinks.k1.flushInterval = 10
a1.sinks.k1.labelPrefix = biz

a1.channels.c1.type = file
a1.channels.c1.checkpointDir = /data/flume/file-channel/checkpoint
a1.channels.c1.dataDirs = /data/flume/file-channel/data

a1.sources.r1.channels = c1
a1.sinks.k1.channel = c1

```

#### 通过supervisor管理并监控flume进程
* 支持进程监控，重启

```
[program:flume-unique-ng]
command=/root/soft/flume-1.7.0/bin/flume-ng agent -c . -f conf/flume-dosris-unique.conf -n a1 -Dflume.root.logger=ALL
process_name=%(program_name)s
numprocs=1
directory=/root/soft/flume-1.7.0
umask=022
priority=999
autostart=true
autorestart=true
startsecs=10
startretries=20
exitcodes=0,2
stopsignal=TERM
stopasgroup=true
killasgroup=true
stopwaitsecs=10
user=root
redirect_stderr=true
stdout_logfile=/data/logs/supervisor/%(program_name)s.log

```

#### 手动单节点部署并执行agent
```
bin/flume-ng agent -c . -f conf/flume-dosris-unique.conf -n a1 -Dflume.root.logger=ALL
```
