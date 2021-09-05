package com.rao.flume.doris;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author raoshihong
 * @date 2021-09-05 09:27
 */
public class DorisSink extends AbstractSink implements Configurable {


    private Logger logger = LoggerFactory.getLogger(DorisSink.class);

    private String prefix;
    private String suffix;

    /**
     * 从配置文件中获取配置属性值
     * @param context
     */
    @Override
    public void configure(Context context) {
        prefix = context.getString("prefix");
        suffix = context.getString("suffix","_aaaa");
    }

    /**
     * 这个方法会被反复的调用
     * 在这里可以通过channel获取event,然后将event输出到控制台或者kafka或者hdfs进行存储
     * @return
     * @throws EventDeliveryException
     */
    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;
        //1.获取channel
        Channel channel = getChannel();

        //2.通过channel获取事务对象
        Transaction takeTransaction = channel.getTransaction();

        //3.开启take事务
        takeTransaction.begin();

        try {
            Event event = null;
            //4.从channel中获取event
            while (true){
                event = channel.take();
                if (event!=null) {
                    break;
                }
            }

            Map<String, String> headers = event.getHeaders();
            logger.info("headers:{}",headers);

            //5.业务处理
            logger.info(prefix+"___"+new String(event.getBody()) + suffix);

            // 在这里进行curl 命令
            new DorisStreamLoad().sendData(new String(event.getBody()),"any_report","site_visit");


            //6.提交事务
            takeTransaction.commit();

            //7.设置状态
            status = Status.READY;
        }catch (Exception e){
            e.printStackTrace();
            //发生异常,则进行事务回滚
            takeTransaction.rollback();
            status = Status.BACKOFF;
        }finally {
            //关闭事务
            takeTransaction.close();
        }

        return status;
    }


}
