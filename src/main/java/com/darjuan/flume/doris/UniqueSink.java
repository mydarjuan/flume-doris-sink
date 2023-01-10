package com.darjuan.flume.doris;

import com.twmacinta.util.MD5;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

import java.io.UnsupportedEncodingException;

/**
 * @author liujianbo
 * @date 2023-01-08
 * 支持多fe节点, 支持批量event采集
 */
public class UniqueSink extends AbstractSink implements Configurable {
    private int batchSize;
    private Context context;
    private StringBuilder batchBuilder = new StringBuilder();
    private int count = 0;

    public UniqueSink() {
    }

    @Override
    public void configure(Context context) {
        this.context = context;
        this.batchSize = context.getInteger("batchSize", 10);
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
                    String msg = new String(event.getBody());
                    String eventId = getEventId(msg);
                    batchBuilder.append(msg).append(context.getString("separator")).append(eventId).append("\n");
                    ++count;
                    // 攒批 batchSize 时提交
                    if (count == this.batchSize) {
                        flush();
                        count = 0;
                    }
                    continue;
                }
                // 攒批不满 batchSize 时提交
                if (batchBuilder.length() > 1) {
                    flush();
                }
                transaction.commit();
                return status;
            }

        } catch (Exception var11) {
            System.out.println("执行异常" + var11.getMessage());
            transaction.rollback();
            throw new EventDeliveryException("Failed to deliver event: " + event, var11);
        } finally {
            transaction.close();
        }
    }

    private String getEventId(String msg) throws UnsupportedEncodingException {
        MD5 md5 = new MD5();
        md5.Update(msg, null);
        return md5.asHex();
    }

    /**
     * sink消息
     *
     * @throws Exception
     */
    private void flush() throws Exception {
        batchBuilder.deleteCharAt(batchBuilder.length() - 1);
        StreamLoad.sink(batchBuilder.toString(), this.context);
        batchBuilder.setLength(0);
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
