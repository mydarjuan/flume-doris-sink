package com.rao.flume.doris;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author raoshihong
 * @date 2021-09-05 09:27
 */
public class DorisSink extends AbstractSink implements Configurable {
    private Logger logger = LoggerFactory.getLogger(DorisSink.class);

    private String host;
    private int port;
    private String user;
    private String password;
    private String database;
    private String table;
    private String mergeType;
    private String separator;
    private String columns;
    private String format;
    private String jsonPaths;
    private String where;

    /**
     * 从配置文件中获取配置属性值
     *
     * @param context
     */
    @Override
    public void configure(Context context) {
        host = context.getString("host");
        port = context.getInteger("port", 8030);
        user = context.getString("user", "root");
        password = context.getString("password", "");
        database = context.getString("database");
        table = context.getString("table");
        mergeType = context.getString("mergeType");
        separator = context.getString("separator");
        columns = context.getString("columns", "");
        format = context.getString("format", "");
        jsonPaths = context.getString("jsonPaths", "");
        where = context.getString("where", "");

        System.out.printf("配置信息-> " + "host:%s," + "port:%s," + "user:%s," + "db:%s," + "table:%s," + "mergeType:%s," + "separator:%s," + "columns:%s," + "format:%s," + "jsonpaths:%s," + "where:%s%n", host, port, user, database, table, mergeType, separator, columns, format, jsonPaths, where);
    }

    /**
     * 这个方法会被反复的调用
     * 在这里可以通过channel获取event,然后将event输出到控制台或者kafka或者hdfs进行存储
     *
     * @return
     * @throws EventDeliveryException
     */
    @Override
    public Status process() throws EventDeliveryException {
        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();
        Event event = null;
        txn.begin();

        while (true) {
            event = ch.take();
            if (event != null) {
                break;
            }
        }

        try {
            String body = new String(event.getBody());
            new DorisStreamLoad().sendData(body, host, port, user, password, database, table, mergeType, separator, columns, format, jsonPaths, where);
            txn.commit();
            return Status.READY;
        } catch (Throwable th) {
            System.out.println("异常信息:" + th.getMessage());
            txn.rollback();
            if (th instanceof Error) {
                throw (Error) th;
            } else {
                throw new EventDeliveryException(th);
            }
        } finally {
            txn.close();
        }
    }
}
