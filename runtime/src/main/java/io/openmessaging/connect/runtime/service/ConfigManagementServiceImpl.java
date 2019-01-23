package io.openmessaging.connect.runtime.service;

import io.openmessaging.KeyValue;
import io.openmessaging.connect.runtime.ConnAndTaskConfigs;
import io.openmessaging.connect.runtime.store.ConnectorConfigFileBaseStore;
import io.openmessaging.connect.runtime.store.MetaStore;
import io.openmessaging.connect.runtime.utils.BrokerBasedLog;
import io.openmessaging.connect.runtime.utils.Callback;
import io.openmessaging.connect.runtime.utils.DataSynchronizer;
import java.util.List;
import java.util.Map;

public class ConfigManagementServiceImpl implements ConfigManagementService {

    private MetaStore metaStore;
    private ConnectorConfigUpdateListener connectorConfigUpdateListener;
    private DataSynchronizer<String, ConnAndTaskConfigs> dataSynchronizer;

    public ConfigManagementServiceImpl(ConnectorConfigUpdateListener connectorConfigUpdateListener){
        this.metaStore = new ConnectorConfigFileBaseStore();
        this.connectorConfigUpdateListener = connectorConfigUpdateListener;
        this.dataSynchronizer = new BrokerBasedLog<>();
    }

    @Override public void start() {
        dataSynchronizer.start(new ConfigChangeCallback());
    }

    @Override public void stop() {
        dataSynchronizer.stop();
    }

    @Override public Map<String, KeyValue> getConnectorConfigs() {
        return ((ConnAndTaskConfigs)metaStore.getData()).getConnectorConfigs();
    }

    @Override public void putConnectorConfig(String connectorName, KeyValue configs) {

    }

    @Override public void removeConnectorConfig(String connectorName) {

    }

    @Override public Map<String, List<KeyValue>> getTaskConfigs() {
        return ((ConnAndTaskConfigs)metaStore.getData()).getTaskConfigs();
    }

    @Override public void putTaskConfigs(String connectorName, List<KeyValue> configs) {

    }

    @Override public void removeTaskConfigs(String connectorName) {

    }

    @Override public void persist() {
        this.metaStore.persist();
    }

    private class ConfigChangeCallback implements Callback<String, ConnAndTaskConfigs> {

        @Override public void onCompletion(Throwable error, String key, ConnAndTaskConfigs result) {
            connectorConfigUpdateListener.onConfigUpdate();
        }
    }
}
