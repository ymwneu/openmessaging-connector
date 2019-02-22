package io.openmessaging.connect.runtime.connectorwrapper;

import com.alibaba.fastjson.JSON;
import io.openmessaging.connect.runtime.common.ConnectKeyValue;
import io.openmessaging.connector.api.Connector;

public class WorkerConnector{

    private String connectorName;
    private Connector connector;
    private ConnectKeyValue keyValue;
    public WorkerConnector(String connectorName, Connector connector, ConnectKeyValue keyValue) {
        this.connectorName = connectorName;
        this.connector = connector;
        this.keyValue = keyValue;
    }

    public void start() {
        connector.verifyAndSetConfig(keyValue);
        connector.start();
    }

    public void stop(){
        connector.stop();
    }

    public String getConnectorName() {
        return connectorName;
    }

    public ConnectKeyValue getKeyValue() {
        return keyValue;
    }

    public void reconfigure(ConnectKeyValue keyValue){
        this.keyValue = keyValue;
        stop();
        start();
    }

    @Override
    public String toString(){

        StringBuilder sb = new StringBuilder();
        sb.append("connectorName:"+connectorName)
            .append("\nConfigs:"+ JSON.toJSONString(keyValue));
        return sb.toString();
    }
}
