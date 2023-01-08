package com.rao.flume.doris;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

import java.util.List;

/**
 * @author raoshihong
 * @date 2021-09-05 09:27
 */
public class DorisSink extends AbstractSink implements Configurable {
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
            DorisStreamLoad.sink(new String(event.getBody()), context);
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
