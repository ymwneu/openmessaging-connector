package io.openmessaging.connect.runtime.common;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConnAndTaskConfigs {

    private Map<String, ConnectKeyValue> connectorConfigs = new HashMap<>();
    private Map<String, List<ConnectKeyValue>> taskConfigs = new HashMap<>();

    public Map<String, ConnectKeyValue> getConnectorConfigs() {
        return connectorConfigs;
    }

    public void setConnectorConfigs(Map<String, ConnectKeyValue> connectorConfigs) {
        this.connectorConfigs = connectorConfigs;
    }

    public Map<String, List<ConnectKeyValue>> getTaskConfigs() {
        return taskConfigs;
    }

    public void setTaskConfigs(Map<String, List<ConnectKeyValue>> taskConfigs) {
        this.taskConfigs = taskConfigs;
    }
}
