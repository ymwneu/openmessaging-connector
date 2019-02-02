package org.apache.rocketmq.mysql.connector;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.Task;
import io.openmessaging.connector.api.source.SourceConnector;
import io.openmessaging.internal.DefaultKeyValue;
import java.util.ArrayList;
import java.util.List;

public class MysqlConnector extends SourceConnector {

    private KeyValue config;

    @Override public void start(KeyValue config) {
        this.config = config;
    }

    @Override public void stop() {

    }

    @Override public Class<? extends Task> taskClass() {
        return MysqlTask.class;
    }

    @Override public List<KeyValue> taskConfigs() {
        List<KeyValue> config = new ArrayList<>();
        config.add(new DefaultKeyValue());
        return config;
    }
}
