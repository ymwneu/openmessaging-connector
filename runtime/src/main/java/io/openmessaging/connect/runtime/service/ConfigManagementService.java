package io.openmessaging.connect.runtime.service;

import io.openmessaging.connect.runtime.common.ConnectKeyValue;
import java.util.List;
import java.util.Map;

public interface ConfigManagementService {

    void start();

    void stop();

    Map<String, ConnectKeyValue> getConnectorConfigs();

    String putConnectorConfig(String connectorName, ConnectKeyValue configs) throws Exception;

    void removeConnectorConfig(String connectorName);

    Map<String, List<ConnectKeyValue>> getTaskConfigs();

    void persist();

    void registerListener(ConnectorConfigUpdateListener listener);

    interface ConnectorConfigUpdateListener {
        void onConfigUpdate();
    }

    interface TaskConfigUpdateListener {
        void onConfigUpdate();
    }
}
