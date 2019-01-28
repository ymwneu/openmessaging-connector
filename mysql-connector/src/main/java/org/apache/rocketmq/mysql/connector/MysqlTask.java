package org.apache.rocketmq.mysql.connector;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.sink.OMSQueue;
import io.openmessaging.connector.api.source.SourceDataEntry;
import io.openmessaging.connector.api.source.SourceTask;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.mysql.Replicator;
import org.apache.rocketmq.mysql.binlog.Transaction;

public class MysqlTask extends SourceTask {

    private Replicator replicator;

    @Override public Collection<SourceDataEntry> poll() {
        SourceDataEntry sourceDataEntry = new SourceDataEntry();
        try {
            Transaction transaction = replicator.getQueue().poll(1000, TimeUnit.MILLISECONDS);
            OMSQueue queue = new OMSQueue();
            queue.setQueueName("mysql");
            sourceDataEntry.setQueue(queue);
            sourceDataEntry.setPayload(transaction.toJson());

        } catch (Exception e) {

        }
        List<SourceDataEntry> res = new ArrayList<>();
        if(null != sourceDataEntry.getPayload()){
            res.add(sourceDataEntry);
        }
        return res;
    }

    @Override public void start(KeyValue props) {

        replicator = new Replicator();
        replicator.start();
    }

    @Override public void stop() {
        replicator.stop();
    }
}
