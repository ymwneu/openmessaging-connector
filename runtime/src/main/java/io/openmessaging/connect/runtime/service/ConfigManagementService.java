package io.openmessaging.connect.runtime.service;

import io.openmessaging.KeyValue;
import java.util.List;
import java.util.Map;

public interface ConfigManagementService {

    void start();

    void stop();

    Map<String, KeyValue> getConnectorConfigs();

    void putConnectorConfig(String connectorName, KeyValue configs) throws Exception;

    void removeConnectorConfig(String connectorName);

    Map<String, List<KeyValue>> getTaskConfigs();

    void persist();

    void registerListener(ConnectorConfigUpdateListener listener);

    interface ConnectorConfigUpdateListener {
        void onConfigUpdate();
    }

    interface TaskConfigUpdateListener {
        void onConfigUpdate();
    }
}
