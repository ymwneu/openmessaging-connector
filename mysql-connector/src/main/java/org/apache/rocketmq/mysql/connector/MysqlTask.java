package org.apache.rocketmq.mysql.connector;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.OMS;
import io.openmessaging.connector.api.ConfigDefine;
import io.openmessaging.connector.api.source.SourceTask;
import io.openmessaging.producer.Producer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.mysql.Replicator;
import org.apache.rocketmq.mysql.binlog.Transaction;

public class MysqlTask extends SourceTask {

    private Replicator replicator;
    Producer producer;

    @Override public Collection<Message> poll() {

        List<Message> res = new ArrayList<>();

        try {
            Transaction transaction = replicator.getQueue().poll(1000, TimeUnit.MILLISECONDS);
            Message sourceMessage = producer.createBytesMessage("mysql", transaction.toJson().getBytes());
            res.add(sourceMessage);
        } catch (Exception e) {

        }
        return res;
    }

    @Override public void start(KeyValue props) {

        replicator = new Replicator();
        replicator.start();
        producer =
            OMS.getMessagingAccessPoint(props.getString(ConfigDefine.OMS_DRIVER_URL)).createProducer();
    }

    @Override public void stop() {
        replicator.stop();
    }
}
