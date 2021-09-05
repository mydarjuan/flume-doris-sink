package com.rao.flume.doris;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
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
    private final static String DORIS_HOST_DEV = "hadoop101";

    public static Logger logger = LoggerFactory.getLogger(DorisStreamLoad.class);

    //    private final static String DORIS_TABLE = "join_test";
    private final static String DORIS_USER = "root";
    private final static String DORIS_PASSWORD = "123456";
    private final static int DORIS_HTTP_PORT = 8030;

    public void sendData(String content, String db, String table) throws Exception {

        final String loadUrl = String.format("http://%s:%s/api/%s/%s/_stream_load",
            DORIS_HOST_DEV,
            DORIS_HTTP_PORT,
            db,
            table);
        final HttpClientBuilder httpClientBuilder = HttpClients
            .custom()
            .setRedirectStrategy(new DefaultRedirectStrategy() {
                @Override
                protected boolean isRedirectable(String method) {
                    return true;
                }
            });
        CloseableHttpClient client = httpClientBuilder.build();

        HttpPut put = new HttpPut(loadUrl);
        StringEntity entity = new StringEntity(content, "UTF-8");
        put.setHeader(HttpHeaders.EXPECT, "100-continue");
        put.setHeader(HttpHeaders.AUTHORIZATION, basicAuthHeader(DORIS_USER, DORIS_PASSWORD));

        put.setHeader("strip_outer_array", "true");
//        put.setHeader("format", "json");
        put.setHeader("merge_type", "MERGE");
//        put.setHeader("delete", "canal_type=\"DELETE\"");
                    put.setHeader("label", UUID.randomUUID().toString());
        put.setHeader("column_separator",",");
        put.setEntity(entity);


        reConnect(client, put);

    }

    private void reConnect(CloseableHttpClient client, HttpPut put) throws Exception {


        String loadResult = "";
        CloseableHttpResponse response = client.execute(put);
        //todo 调用方法
        if (response.getEntity() != null) {

            loadResult = EntityUtils.toString(response.getEntity());
        }
        final int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != 200) {
            System.out.println("写入失败");
        } else {
            if (loadResult.contains("OK") && loadResult.contains("Success")) {
                System.out.println(loadResult);
            } else if (loadResult.contains("Fail")) {
                throw new Exception(loadResult + ",抛出异常,任务失败，当前时间: " + System.currentTimeMillis());
            } else {
                throw new Exception(loadResult + ",抛出异常,任务失败，当前时间: " + System.currentTimeMillis());
            }

        }


    }


    private String basicAuthHeader(String username, String password) {
        final String tobeEncode = username + ":" + password;
        byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded);
    }

}
