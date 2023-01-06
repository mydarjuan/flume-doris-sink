# flume-doris

flume sink for doris by stream load


# flume sample config
a1.sources = r1
a1.channels = c1
a1.sinks = k1

# Describe/configure the source
#source的类型为TAILDIR，这里的类型大小写都可以
a1.sources.r1.type = taildir
a1.sources.r1.channels = c1
#存储tial最后一个位置存储位置
a1.sources.r1.positionFile = /data/flume/taildir_position.json
#设置tiail的组， 使用空格隔开
a1.sources.r1.filegroups = f1 f2
#设置每个分组的绝对路径
a1.sources.r1.filegroups.f1 = /data/flume/test.txt
#.匹配除换行符 \n 之外的任何单字符。*匹配前面的子表达式零次或多次。这里也可以用messages.*
a1.sources.r1.filegroups.f2 = /data/flume/test2/.*
#是否在header中添加文件的完整路径信息
a1.sources.r1.fileHeader = true

# Describe the sink
a1.sinks.k1.type = com.rao.flume.doris.DorisSink
a1.sinks.k1.host = 172.20.1.91
a1.sinks.k1.port = 8030
a1.sinks.k1.user = root
a1.sinks.k1.password = 
a1.sinks.k1.database = db_log
a1.sinks.k1.table = error_log
a1.sinks.k1.mergeType = APPEND
a1.sinks.k1.separator = ,
a1.sinks.k1.format = 
a1.sinks.k1.jsonPaths = 
a1.sinks.k1.where = 

# Use a channel which buffers events in memory
a1.channels.c1.type = memory
a1.channels.c1.capacity = 1000
a1.channels.c1.transactionCapacity = 100

# Bind the source and sink to the channel
a1.sources.r1.channels = c1
a1.sinks.k1.channel = c1
