package com.darjuan.flume.doris.service;

import com.google.common.base.Preconditions;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

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
            System.out.println("主机:" + host + "链接失败");
            return false;
        }
    }

    /**
     * 随机获取fe节点
     *
     * @param options
     * @return
     */
    private static String getLoadHost(Options options) {
        String[] hostList = options.getHosts();
        String host = "http://" + hostList[new Random().nextInt(hostList.length)] + ":" + options.getPort();
        if (checkConnection(host)) {
            return host;
        }
        return null;
    }

    /**
     * 同步消息
     *
     * @param data
     * @param options
     * @throws Exception
     */
    public static void sink(String data, Options options) throws Exception {
        String host = getLoadHost(options);
        Preconditions.checkArgument(StringUtils.isNotEmpty(host), "无可用fe节点!");
        final String loadUrl = String.format("%s/api/%s/%s/_stream_load", host, options.getDatabase(), options.getTable());
        final HttpClientBuilder httpClientBuilder = HttpClients.custom().setRedirectStrategy(new DefaultRedirectStrategy() {
            @Override
            protected boolean isRedirectable(String method) {
                return true;
            }
        });
        HttpPut put = builderEntity(loadUrl, data, options);
        loadData(loadUrl, httpClientBuilder.build(), put);
    }

    /**
     * 构建消息体
     *
     * @param loadUrl
     * @param data
     * @param options
     * @return
     */
    private static HttpPut builderEntity(String loadUrl, String data, Options options) {
        HttpPut put = new HttpPut(loadUrl);
        put.setHeader(HttpHeaders.EXPECT, "100-continue");
        put.setHeader(HttpHeaders.AUTHORIZATION, basicAuthHeader(options.getUsername(), options.getPassword()));
        if (StringUtils.isNotEmpty(options.getMergeType())) {
            put.setHeader("merge_type", options.getMergeType());
        }
        put.setHeader("label", options.getLabelPrefix() + UUID.randomUUID().toString() + System.currentTimeMillis());
        if (StringUtils.isNotEmpty(options.getSeparator())) {
            put.setHeader("column_separator", options.getSeparator());
        }
        if (StringUtils.isNotEmpty(options.getColumns())) {
            put.setHeader("columns", options.getColumns());
        }
        if (StringUtils.isNotEmpty(options.getRowFormat())) {
            put.setHeader("format", options.getRowFormat());
            put.setHeader("jsonpaths", options.getJsonPaths());

        }
        if (StringUtils.isNotEmpty(options.getRowsOnLine())) {
            put.setHeader("read_json_by_line", "true");
        } else {
            put.setHeader("strip_outer_array", "true");
        }

        if (StringUtils.isNotEmpty(options.getSqlWhere())) {
            put.setHeader("where", options.getSqlWhere());
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
