package io.openmessaging.connect.runtime.service;

import io.openmessaging.KeyValue;
import io.openmessaging.MessagingAccessPoint;
import io.openmessaging.connect.runtime.ConnAndTaskConfigs;
import io.openmessaging.connect.runtime.store.ConnectorConfigFileBaseStore;
import io.openmessaging.connect.runtime.store.MemoryStore;
import io.openmessaging.connect.runtime.store.MetaStore;
import io.openmessaging.connect.runtime.utils.BrokerBasedLog;
import io.openmessaging.connect.runtime.utils.Callback;
import io.openmessaging.connect.runtime.utils.DataSynchronizer;
import io.openmessaging.connector.api.Connector;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ConfigManagementServiceImpl implements ConfigManagementService {

    private MetaStore<ConnAndTaskConfigs> metaStore;
    private Set<ConnectorConfigUpdateListener> connectorConfigUpdateListener;
    private DataSynchronizer<String, ConnAndTaskConfigs> dataSynchronizer;

    public ConfigManagementServiceImpl(MessagingAccessPoint point){
        this.metaStore = new MemoryStore();
        this.connectorConfigUpdateListener = new HashSet<>();
        this.dataSynchronizer = null;

        metaStore.setData(new ConnAndTaskConfigs());
    }

    @Override public void start() {
        dataSynchronizer.start();
    }

    @Override public void stop() {
        dataSynchronizer.stop();
    }

    @Override public Map<String, KeyValue> getConnectorConfigs() {

        return metaStore.getData().getConnectorConfigs();
    }

    @Override public void putConnectorConfig(String connectorName, KeyValue configs) throws Exception {

        KeyValue exist = metaStore.getData().getConnectorConfigs().get(connectorName);
        if(configs.equals(exist)){
            return;
        }
        String className = configs.getString("class");
        Class clazz = Class.forName(className);
        Connector connector = (Connector) clazz.newInstance();
        connector.initConfiguration(configs);
        connector.start();
        metaStore.getData().getConnectorConfigs().put(connectorName, configs);
        List<KeyValue> taskConfigs = connector.taskConfigs();
        for(KeyValue keyValue : taskConfigs){
            keyValue.put("class", connector.taskClass().getName());
        }
        putTaskConfigs(connectorName, taskConfigs);
        connector.stop();
        if(null == this.connectorConfigUpdateListener){
            return;
        }
        for(ConnectorConfigUpdateListener listener : this.connectorConfigUpdateListener){
            listener.onConfigUpdate();
        }
    }

    @Override public void removeConnectorConfig(String connectorName) {

    }

    @Override public Map<String, List<KeyValue>> getTaskConfigs() {

        return metaStore.getData().getTaskConfigs();
    }

    @Override public void putTaskConfigs(String connectorName, List<KeyValue> configs) {

        List<KeyValue> exist = metaStore.getData().getTaskConfigs().get(connectorName);
        if(configs.equals(exist)){
            return;
        }
        metaStore.getData().getTaskConfigs().put(connectorName, configs);
    }

    @Override public void removeTaskConfigs(String connectorName) {

    }

    @Override public void persist() {
        this.metaStore.persist();
    }

    @Override public void registerListener(ConnectorConfigUpdateListener listener) {
        this.connectorConfigUpdateListener.add(listener);
    }

    private class ConfigChangeCallback implements Callback<String, ConnAndTaskConfigs> {

        @Override public void onCompletion(Throwable error, String key, ConnAndTaskConfigs result) {
            if(null == ConfigManagementServiceImpl.this.connectorConfigUpdateListener){
                return;
            }
            for(ConnectorConfigUpdateListener listener : ConfigManagementServiceImpl.this.connectorConfigUpdateListener){
                listener.onConfigUpdate();
            }
        }
    }
}
