package com.darjuan.flume.doris.service;

import org.apache.flume.Context;

import java.io.Serializable;

public class Options implements Serializable {

    private final Context context;

    private static final String USERNAME = "user";
    private static final String PASSWORD = "password";
    private static final String DATABASE = "database";
    private static final String TABLE = "table";
    private static final String COLUMNS = "columns";
    private static final String MAX_BATCH_SIZE = "batchSize";
    private static final String FLUSH_INTERVAL = "flushInterval";
    private static final String HOSTS = "hosts";
    private static final String PORT = "port";
    private static final String UNIQUE_EVENT = "uniqueEvent";
    private static final String MERGE_TYPE = "mergeType";
    private static final String SEPARATOR = "separator";
    private static final String FORMAT = "format";
    private static final String JSON_PATHS = "jsonPaths";
    private static final String BY_LINE = "byLine";
    private static final String WHERE = "where";
    private static final String LABEL_PREFIX = "labelPrefix";


    public Options(Context context) {
        this.context = context;
    }

    public String getDatabase() {
        return context.getString(DATABASE);
    }

    public String getTable() {
        return context.getString(TABLE);
    }

    public String getUsername() {
        return context.getString(USERNAME);
    }

    public String getPassword() {
        return context.getString(PASSWORD,"");
    }

    public String[] getHosts() {
        return context.getString(HOSTS).split(",");
    }

    public Integer getPort() {
        return context.getInteger(PORT, 8030);
    }

    public String getColumns() {
        return context.getString(COLUMNS);
    }

    public Integer getBatchSize() {
        return context.getInteger(MAX_BATCH_SIZE, 1);
    }

    public Integer getFlushInterval() {
        return context.getInteger(FLUSH_INTERVAL, 0);
    }

    public Integer getUniqueEvent() {
        return context.getInteger(UNIQUE_EVENT, 0);
    }

    public String getMergeType() {
        return context.getString(MERGE_TYPE, "APPEND");
    }

    public String getSeparator() {
        return context.getString(SEPARATOR, ",");
    }

    public String getRowFormat() {
        return context.getString(FORMAT);
    }

    public String getJsonPaths() {
        return context.getString(JSON_PATHS);
    }

    public String getRowsOnLine() {
        return context.getString(BY_LINE, "true");
    }

    public String getSqlWhere() {
        return context.getString(WHERE);
    }

    public String getLabelPrefix() {
        return context.getString(LABEL_PREFIX);
    }
}
