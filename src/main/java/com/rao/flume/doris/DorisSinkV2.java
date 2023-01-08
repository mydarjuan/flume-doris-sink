package com.rao.flume.doris;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

/**
 * @author liujianbo
 * @date 2023-01-08
 */
public class DorisSinkV2 extends AbstractSink implements Configurable {
    private int batchSize;
    private Context context;
    private StringBuilder batchBuilder = new StringBuilder();
    private int count = 0;

    public DorisSinkV2() {
    }

    @Override
    public void configure(Context context) {
        this.context = context;
        this.batchSize = context.getInteger("batchSize", 10);
    }

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
                    batchBuilder.append(new String(event.getBody())).append("\n");
                    ++count;

                    if (count == this.batchSize) {
                        batchBuilder.deleteCharAt(batchBuilder.length() - 1);
                        DorisStreamLoad.sink(batchBuilder.toString(), context);
                        batchBuilder.setLength(0);
                        count = 0;
                    }

                    continue;
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

    @Override
    public void start() {
        super.start();
    }

    @Override
    public void stop() {
        super.stop();
    }
}
