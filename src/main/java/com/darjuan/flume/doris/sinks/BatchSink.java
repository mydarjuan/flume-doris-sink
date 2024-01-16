package com.darjuan.flume.doris.sinks;

import com.darjuan.flume.doris.service.Options;
import com.darjuan.flume.doris.service.StreamLoad;
import com.google.common.collect.Lists;
import com.twmacinta.util.MD5;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * @author liujianbo
 * @date 2023-01-08
 * 支持多fe节点, 支持单个，批量，唯一event采集
 */
public class BatchSink extends AbstractSink implements Configurable {
    /**
     * 配置
     */
    private Options options;

    /**
     * 批量消息
     */
    private final StringBuilder batchEvents = new StringBuilder();

    /**
     * 计数器
     */
    private SinkCounter sinkCounter;

    @Override
    public void configure(Context context) {
        System.out.println("初始化配置...");
        options = new Options(context);
        options.validateRequired();
        if (this.sinkCounter == null) {
            this.sinkCounter = new SinkCounter(this.getName());
        }
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = Status.READY;
        Channel channel = this.getChannel();
        Transaction transaction = channel.getTransaction();

        try {
            transaction.begin();
            List<Event> batch = Lists.newLinkedList();
            int size;
            for (size = 0; size < this.options.getBatchSize(); ++size) {
                Event event = channel.take();
                if (event == null) {
                    break;
                }
                batch.add(event);
                batchEvent(event);
            }

            size = batch.size();
            int batchSize = this.options.getBatchSize();
            if (size == 0) {
                this.sinkCounter.incrementBatchEmptyCount();
                status = Status.BACKOFF;
            } else {
                if (size < batchSize) {
                    this.sinkCounter.incrementBatchUnderflowCount();
                } else {
                    this.sinkCounter.incrementBatchCompleteCount();
                }
                this.sinkCounter.addToEventDrainAttemptCount((long) size);
                flushEvent();
            }

            transaction.commit();
            this.sinkCounter.addToEventDrainSuccessCount((long) size);
        } catch (Throwable var10) {

            System.out.println(var10.getMessage());

            transaction.rollback();
            if (var10 instanceof Error) {
                throw (Error) var10;
            }

            if (!(var10 instanceof ChannelException)) {
                clearEvents();
                throw new EventDeliveryException("Failed to send events", var10);
            }

            status = Status.BACKOFF;
        } finally {
            transaction.close();
        }

        return status;
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
        StreamLoad.sink(batchEvents.toString(), this.options);
    }

    /**
     * 前置处理
     */
    private void beforeFlush() {
        batchEvents.deleteCharAt(batchEvents.length() - 1);
    }

    /**
     * 后续清理
     *
     * @throws InterruptedException
     */
    private void afterFlush() throws InterruptedException {
        batchEvents.setLength(0);
        if (this.options.getFlushInterval() > 0) {
            Thread.sleep(this.options.getFlushInterval());
        }
    }

    /**
     * 消息清理
     */
    private void clearEvents() {
        batchEvents.setLength(0);
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
            batchEvents.append(eventId).append(options.getSeparator()).append(msg).append("\n");
        } else {
            batchEvents.append(msg).append("\n");
        }
    }

    @Override
    public void start() {
        this.sinkCounter.start();
        super.start();
    }

    @Override
    public void stop() {
        this.sinkCounter.stop();
        super.stop();
    }
}
