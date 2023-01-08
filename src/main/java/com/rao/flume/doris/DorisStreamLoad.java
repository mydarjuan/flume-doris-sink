package com.rao.flume.doris;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
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

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * @author raoshihong
 * @date 2021-09-05 16:33
 */
public class DorisStreamLoad {

    public void sendData(String data, String host, String user, String pwd, String db, String table, String mergeType, String separator, String columns, String format, String jsonPaths, String where) throws Exception {

        final String loadUrl = String.format("%s/api/%s/%s/_stream_load", host, db, table);

        final HttpClientBuilder httpClientBuilder = HttpClients.custom().setRedirectStrategy(new DefaultRedirectStrategy() {
            @Override
            protected boolean isRedirectable(String method) {
                return true;
            }
        });

        HttpPut put = builderEntity(loadUrl, data, user, pwd, mergeType, separator, columns, format, jsonPaths, where);

        put(httpClientBuilder.build(), put);
    }

    private HttpPut builderEntity(String loadUrl, String data, String user, String pwd, String mergeType, String separator, String columns, String format, String jsonPaths, String where) {
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

    private void put(CloseableHttpClient client, HttpPut put) throws Exception {
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
            System.out.println(loadResult);
        } else if (loadResult.contains("Fail")) {
            throw new Exception(loadResult + ",抛出异常,任务失败，当前时间: " + System.currentTimeMillis());
        } else {
            throw new Exception(loadResult + ",抛出异常,任务失败，当前时间: " + System.currentTimeMillis());
        }
    }

    private String basicAuthHeader(String username, String password) {
        final String tobeEncode = username + ":" + password;
        byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded);
    }

}
