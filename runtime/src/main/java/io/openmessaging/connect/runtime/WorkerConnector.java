package io.openmessaging.connect.runtime;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.Connector;

public class WorkerConnector{

    private String connectorName;
    private Connector connector;
    private KeyValue keyValue;
    public WorkerConnector(String connectorName, Connector connector, KeyValue keyValue) {
        this.connectorName = connectorName;
        this.connector = connector;
        this.keyValue = keyValue;
    }

    public void start() {
        connector.initConfiguration(keyValue);
        connector.start();
    }

    public void stop(){
        connector.stop();
    }

    public String getConnectorName() {
        return connectorName;
    }

    public KeyValue getKeyValue() {
        return keyValue;
    }

    public void reconfigure(KeyValue keyValue){
        this.keyValue = keyValue;
        stop();
        start();
    }
}
