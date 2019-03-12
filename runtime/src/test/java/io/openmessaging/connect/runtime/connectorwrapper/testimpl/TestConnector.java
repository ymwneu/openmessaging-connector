package io.openmessaging.connect.runtime.connectorwrapper.testimpl;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.Connector;
import io.openmessaging.connector.api.Task;

import java.util.List;

public class TestConnector implements Connector {

    @Override
    public String verifyAndSetConfig(KeyValue config) {
        return null;
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public void pause() {

    }

    @Override
    public void resume() {

    }

    @Override
    public Class<? extends Task> taskClass() {
        return null;
    }

    @Override
    public List<KeyValue> taskConfigs() {
        return null;
    }
}
