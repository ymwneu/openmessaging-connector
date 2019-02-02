package io.openmessaging.connect.runtime;

import io.openmessaging.KeyValue;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConnAndTaskConfigs {

    private Map<String, KeyValue> connectorConfigs = new HashMap<>();
    private Map<String, List<KeyValue>> taskConfigs = new HashMap<>();

    public Map<String, KeyValue> getConnectorConfigs() {
        return connectorConfigs;
    }

    public void setConnectorConfigs(Map<String, KeyValue> connectorConfigs) {
        this.connectorConfigs = connectorConfigs;
    }

    public Map<String, List<KeyValue>> getTaskConfigs() {
        return taskConfigs;
    }

    public void setTaskConfigs(Map<String, List<KeyValue>> taskConfigs) {
        this.taskConfigs = taskConfigs;
    }
}
