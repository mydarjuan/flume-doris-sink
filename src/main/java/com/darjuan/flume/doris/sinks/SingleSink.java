package com.darjuan.flume.doris.sinks;

import com.darjuan.flume.doris.service.StreamLoad;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

/**
 * @author liujianbo
 * @date 2023-01-08
 * 支持多fe节点, 单个event采集
 */
public class SingleSink extends AbstractSink implements Configurable {
    private Context context;

    @Override
    public void configure(Context context) {
        this.context = context;
    }

    @Override
    public Status process() throws EventDeliveryException {
        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();
        Event event;
        txn.begin();

        do {
            event = ch.take();
        } while (event == null);

        try {
            StreamLoad.sink(new String(event.getBody()), context);
            txn.commit();
            return Status.READY;
        } catch (Throwable th) {
            System.out.println("异常信息:" + th.getMessage());
            txn.rollback();
            if (th instanceof Error) {
                throw (Error) th;
            } else {
                throw new EventDeliveryException(th);
            }
        } finally {
            txn.close();
        }
    }
}
