# flume sample config
a1.sources = r1
a1.channels = c1
a1.sinks = k1

# Describe/configure the source
a1.sources.r1.type = taildir
a1.sources.r1.channels = c1
a1.sources.r1.positionFile = /data/flume/taildir_position.json
a1.sources.r1.filegroups = f1 f2
a1.sources.r1.filegroups.f1 = /data/flume/test.txt
a1.sources.r1.filegroups.f2 = /data/flume/test2/.*
#a1.sources.r1.batchSize = 100
a1.sources.r1.fileHeader = true


# Describe the sink
a1.sinks.k1.type = com.darjuan.flume.doris.BatchSink
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

# Use a channel which buffers events in memory
a1.channels.c1.type = file
a1.channels.c1.checkpointDir = /data/flume/file-channel/checkpoint
a1.channels.c1.dataDirs = /data/flume/file-channel/data

#a1.channels.c1.type = memory
#a1.channels.c1.capacity = 1000
#a1.channels.c1.transactionCapacity = 100

# Bind the source and sink to the channel
a1.sources.r1.channels = c1
a1.sinks.k1.channel = c1
