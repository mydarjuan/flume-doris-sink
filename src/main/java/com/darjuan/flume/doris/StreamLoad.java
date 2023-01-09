package com.darjuan.flume.doris;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.UUID;

public class StreamLoad {

    /**
     * 检查服务可用性
     *
     * @param host
     * @return
     */
    private static boolean checkConnection(String host) {
        try {
            URL url = new URL(host);
            HttpURLConnection co = (HttpURLConnection) url.openConnection();
            co.setConnectTimeout(5000);
            co.connect();
            co.disconnect();
            return true;
        } catch (Exception e1) {
            e1.printStackTrace();
            return false;
        }
    }

    /**
     * 随机获取fe节点
     *
     * @param context
     * @return
     */
    private static String getLoadHost(Context context) {
        String[] hostList = context.getString("hosts").split(",");
        String host = "http://" + hostList[new Random().nextInt(hostList.length)] + ":" + context.getInteger("port", 8030);
        if (checkConnection(host)) {
            return host;
        }
        return null;
    }

    /**
     * 同步消息
     *
     * @param data
     * @param context
     * @throws Exception
     */
    public static void sink(String data, Context context) throws Exception {

        String host = getLoadHost(context);
        String user = context.getString("user", "root");
        String password = context.getString("password", "");
        String database = context.getString("database");
        String table = context.getString("table");
        String mergeType = context.getString("mergeType");
        String separator = context.getString("separator");
        String columns = context.getString("columns", "");
        String format = context.getString("format", "");
        String jsonPaths = context.getString("jsonPaths", "");
        String where = context.getString("where", "");

        final String loadUrl = String.format("%s/api/%s/%s/_stream_load", host, database, table);

        final HttpClientBuilder httpClientBuilder = HttpClients.custom().setRedirectStrategy(new DefaultRedirectStrategy() {
            @Override
            protected boolean isRedirectable(String method) {
                return true;
            }
        });

        HttpPut put = builderEntity(loadUrl, data, user, password, mergeType, separator, columns, format, jsonPaths, where);

        loadData(loadUrl, httpClientBuilder.build(), put);
    }

    /**
     * 构建消息体
     *
     * @param loadUrl
     * @param data
     * @param user
     * @param pwd
     * @param mergeType
     * @param separator
     * @param columns
     * @param format
     * @param jsonPaths
     * @param where
     * @return
     */
    private static HttpPut builderEntity(String loadUrl, String data, String user, String pwd, String mergeType, String separator, String columns, String format, String jsonPaths, String where) {
        HttpPut put = new HttpPut(loadUrl);
        put.setHeader(HttpHeaders.EXPECT, "100-continue");
        put.setHeader(HttpHeaders.AUTHORIZATION, basicAuthHeader(user, pwd));
        if (StringUtils.isNotEmpty(mergeType)) {
            put.setHeader("merge_type", mergeType);
        }
        put.setHeader("label", UUID.randomUUID().toString() + System.currentTimeMillis());
        if (StringUtils.isNotEmpty(separator)) {
            put.setHeader("column_separator", separator);
        }
        if (StringUtils.isNotEmpty(columns)) {
            put.setHeader("columns", columns);
        }
        if (StringUtils.isNotEmpty(format)) {
            put.setHeader("format", format);
            put.setHeader("jsonpaths", jsonPaths);
        }
        if (StringUtils.isNotEmpty(where)) {
            put.setHeader("where", where);
        }
        StringEntity entity = new StringEntity(data, "UTF-8");
        put.setEntity(entity);

        return put;
    }

    /**
     * 导入doris
     *
     * @param loadUrl
     * @param client
     * @param put
     * @throws Exception
     */
    private static void loadData(String loadUrl, CloseableHttpClient client, HttpPut put) throws Exception {
        String loadResult = "";
        CloseableHttpResponse response = client.execute(put);
        if (response.getEntity() != null) {
            loadResult = EntityUtils.toString(response.getEntity());
        }
        final int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != 200) {
            throw new Exception("通信异常,任务失败，当前时间: " + System.currentTimeMillis());
        }
        if (loadResult.contains("OK") && loadResult.contains("Success")) {
            System.out.println(loadUrl + "\n" + loadResult);
        } else if (loadResult.contains("Fail")) {
            throw new Exception(loadResult + ",抛出异常,任务失败，当前时间: " + System.currentTimeMillis());
        } else {
            throw new Exception(loadResult + ",抛出异常,任务失败，当前时间: " + System.currentTimeMillis());
        }
    }

    /**
     * 授权
     *
     * @param username
     * @param password
     * @return
     */
    private static String basicAuthHeader(String username, String password) {
        final String tobeEncode = username + ":" + password;
        byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded);
    }

}
