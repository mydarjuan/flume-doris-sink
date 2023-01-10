package com.darjuan.flume.doris.sinks;

import com.darjuan.flume.doris.service.EventProcess;
import com.darjuan.flume.doris.service.StreamLoad;
import com.twmacinta.util.MD5;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

import java.io.UnsupportedEncodingException;

/**
 * @author liujianbo
 * @date 2023-01-08
 * 支持多fe节点, 支持单个，批量，唯一event采集
 */
public class BatchSink extends AbstractSink implements Configurable, EventProcess {
    public int batchSize = 10;
    public int batchCount = 0;
    public int delayInterval = 0;
    public int uniqueEvent = 0;
    public Context context;
    public StringBuilder batchBuilder = new StringBuilder();

    public BatchSink() {
    }

    @Override
    public void configure(Context context) {
        this.context = context;
        this.batchSize = context.getInteger("batchSize", 10);
        this.delayInterval = context.getInteger("delayInterval", 0);
        this.uniqueEvent = context.getInteger("uniqueEvent", 0);
    }

    /**
     * 消息采集
     *
     * @return
     * @throws EventDeliveryException
     */
    @Override
    public Status process() throws EventDeliveryException {
        Status status = Status.READY;
        Channel channel = this.getChannel();
        Transaction transaction = channel.getTransaction();
        Event event = null;

        try {
            transaction.begin();
            while (true) {
                event = channel.take();
                if (event != null) {
                    batchEvent(event);
                    // 攒批 batchSize 时提交
                    if (batchCount == batchSize) {
                        flushEvent();
                    }
                    continue;
                }
                // 攒批不满 batchSize 时提交
                if (batchBuilder.length() > 1) {
                    flushEvent();
                }
                transaction.commit();
                return status;
            }

        } catch (Exception ex) {
            System.out.println("执行异常: " + ex.getMessage());
            transaction.rollback();
            throw new EventDeliveryException("Failed to deliver event: " + event, ex);
        } finally {
            transaction.close();
        }
    }

    /**
     * sink消息
     *
     * @throws Exception
     */
    private void flushEvent() throws Exception {
        beforeFlush();

        sinkEvent();

        afterFlush();
    }

    /**
     * 消息发送
     *
     * @throws Exception
     */
    private void sinkEvent() throws Exception {
        StreamLoad.sink(batchBuilder.toString(), this.context);
    }

    /**
     * 前置处理
     *
     * @throws InterruptedException
     */
    private void beforeFlush() throws InterruptedException {
        batchBuilder.deleteCharAt(batchBuilder.length() - 1);
    }

    /**
     * 后续清理
     *
     * @throws InterruptedException
     */
    private void afterFlush() throws InterruptedException {
        batchBuilder.setLength(0);
        batchCount = 0;
        if (this.delayInterval > 0) {
            Thread.sleep(delayInterval);
        }
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

    @Override
    public void start() {
        super.start();
    }

    @Override
    public void stop() {
        super.stop();
    }

    @Override
    public void batchEvent(Event event) throws UnsupportedEncodingException {
        String msg = new String(event.getBody());
        if (StringUtils.isEmpty(msg)) {
            return;
        }

        if (uniqueEvent > 0) {
            String eventId = getEventId(msg);
            batchBuilder.append(eventId).append(context.getString("separator")).append(msg).append("\n");
        } else {
            batchBuilder.append(msg).append("\n");
        }

        ++batchCount;
    }
}
