# flume-doris

flume sink for doris by stream load


# flume sample config
a1.sources = r1
a1.channels = c1
a1.sinks = k1

a1.sources.r1.type = taildir
a1.sources.r1.channels = c1
a1.sources.r1.positionFile = /data/flume/taildir_position.json
a1.sources.r1.filegroups = f1 f2
a1.sources.r1.filegroups.f1 = /data/flume/test.txt
a1.sources.r1.filegroups.f2 = /data/flume/test2/.*
a1.sources.r1.fileHeader = true

a1.sinks.k1.type = com.rao.flume.doris.DorisSink
a1.sinks.k1.host = 172.20.1.91
a1.sinks.k1.port = 8030
a1.sinks.k1.user = root
a1.sinks.k1.password = 
a1.sinks.k1.database = db_log
a1.sinks.k1.table = error_log
a1.sinks.k1.mergeType = APPEND
a1.sinks.k1.separator = ,
a1.sinks.k1.separator = columns
a1.sinks.k1.format = 
a1.sinks.k1.jsonPaths = 
a1.sinks.k1.where = 


a1.channels.c1.type = memory
a1.channels.c1.capacity = 1000
a1.channels.c1.transactionCapacity = 100


a1.sources.r1.channels = c1
a1.sinks.k1.channel = c1
