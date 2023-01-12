package com.darjuan.flume.doris.sinks;

import com.darjuan.flume.doris.service.Options;
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
public class BatchSink extends AbstractSink implements Configurable {
    public int batchCount = 0;
    public Options options;
    public StringBuilder batchBuilder = new StringBuilder();


    @Override
    public void configure(Context context) {
        System.out.println("初始化配置...");
        options = new Options(context);
        options.validateRequired();
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
                    if (batchCount == options.getBatchSize()) {
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
            throw new EventDeliveryException("消息消费失败: " + event, ex);
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
        StreamLoad.sink(batchBuilder.toString(), this.options);
    }

    /**
     * 前置处理
     */
    private void beforeFlush() {
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
        if (this.options.getFlushInterval() > 0) {
            Thread.sleep(this.options.getFlushInterval());
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

    /**
     * 批量组装消息
     *
     * @param event
     * @throws UnsupportedEncodingException
     */
    public void batchEvent(Event event) throws UnsupportedEncodingException {
        String msg = new String(event.getBody());
        if (StringUtils.isEmpty(msg)) {
            return;
        }

        if (options.getUniqueEvent() > 0) {
            String eventId = getEventId(msg);
            batchBuilder.append(eventId).append(options.getSeparator()).append(msg).append("\n");
        } else {
            batchBuilder.append(msg).append("\n");
        }
        ++batchCount;
    }

    @Override
    public void start() {
        super.start();
    }

    @Override
    public void stop() {
        super.stop();
    }
}
