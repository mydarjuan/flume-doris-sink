package com.darjuan.flume.doris.sinks;

import com.twmacinta.util.MD5;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;

import java.io.UnsupportedEncodingException;

/**
 * @author liujianbo
 * @date 2023-01-08
 * 支持多fe节点, 支持唯一消息ID
 */
public class UniqueSink extends BatchSink implements Configurable {

    @Override
    public void batchEvent(Event event) throws UnsupportedEncodingException {
        String msg = new String(event.getBody());
        String eventId = getEventId(msg);
        batchBuilder.append(eventId).append(context.getString("separator")).append(msg).append("\n");
    }

    /**
     * 基于消息生成MD5 ID
     *
     * @param msg
     * @return
     * @throws UnsupportedEncodingException
     */
    private String getEventId(String msg) throws UnsupportedEncodingException {
        MD5 md5 = new MD5();
        md5.Update(msg, null);
        return md5.asHex();
    }
}
