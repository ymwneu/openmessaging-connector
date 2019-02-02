package io.openmessaging.connect.runtime.service;

import io.openmessaging.KeyValue;
import io.openmessaging.MessagingAccessPoint;
import io.openmessaging.connect.runtime.ConnAndTaskConfigs;
import io.openmessaging.connect.runtime.config.ConnectConfig;
import io.openmessaging.connect.runtime.store.FileBaseKeyValueBasedKeyValueStore;
import io.openmessaging.connect.runtime.store.KeyValueStore;
import io.openmessaging.connect.runtime.utils.BrokerBasedLog;
import io.openmessaging.connect.runtime.utils.Callback;
import io.openmessaging.connect.runtime.utils.ConnAndTaskConfigConverter;
import io.openmessaging.connect.runtime.utils.ConnectKeyValue;
import io.openmessaging.connect.runtime.utils.DataSynchronizer;
import io.openmessaging.connect.runtime.utils.FilePathConfigUtil;
import io.openmessaging.connect.runtime.utils.JsonConverter;
import io.openmessaging.connector.api.ConfigDefine;
import io.openmessaging.connector.api.Connector;
import io.openmessaging.connector.api.sink.OMSQueue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ConfigManagementServiceImpl implements ConfigManagementService {

    private static final OMSQueue CONFIG_MESSAGE_TOPIC = new OMSQueue("config-topic", 0);

    private KeyValueStore<String, KeyValue> connectorKeyValueStore;
    private KeyValueStore<String, List<KeyValue>> taskKeyValueStore;
    private Set<ConnectorConfigUpdateListener> connectorConfigUpdateListener;
    private DataSynchronizer<String, ConnAndTaskConfigs> dataSynchronizer;
    private ConnectConfig connectConfig;

    public ConfigManagementServiceImpl(ConnectConfig connectConfig,
                                       MessagingAccessPoint messagingAccessPoint){

        this.connectConfig = connectConfig;
        this.connectorConfigUpdateListener = new HashSet<>();
        this.dataSynchronizer = new BrokerBasedLog<>(messagingAccessPoint,
                                                     CONFIG_MESSAGE_TOPIC,
                                                     connectConfig.getWorkerId()+System.currentTimeMillis(),
                                                     new ConfigManagementServiceImpl.ConfigChangeCallback(),
                                                     new JsonConverter(),
                                                     new ConnAndTaskConfigConverter(),
                                                     String.class,
                                                     ConnAndTaskConfigs.class);
        this.connectorKeyValueStore = new FileBaseKeyValueBasedKeyValueStore<>(
                                                     FilePathConfigUtil.getConnectorConfigPath(connectConfig.getStorePathRootDir()));
        this.taskKeyValueStore = new FileBaseKeyValueBasedKeyValueStore<>(
                                                     FilePathConfigUtil.getTaskConfigPath(connectConfig.getStorePathRootDir()));
    }

    @Override
    public void start() {

        connectorKeyValueStore.load();
        taskKeyValueStore.load();
        dataSynchronizer.start();
        sendOnlineConfig();
    }

    @Override
    public void stop() {

        connectorKeyValueStore.persist();
        taskKeyValueStore.persist();
    }

    @Override
    public Map<String, KeyValue> getConnectorConfigs() {

        Map<String, KeyValue> result = new HashMap<>();
        Map<String, KeyValue> connectorConfigs = connectorKeyValueStore.getKVMap();
        for(String connectorName : connectorConfigs.keySet()){
            KeyValue config = connectorConfigs.get(connectorName);
            if(0 != config.getInt(ConfigDefine.CONFIG_DELETED)){
                continue;
            }
            result.put(connectorName, config);
        }
        return result;
    }

    @Override
    public void putConnectorConfig(String connectorName, KeyValue configs) throws Exception {

        KeyValue exist = connectorKeyValueStore.get(connectorName);
        if(configs.equals(exist)){
            return;
        }
        String className = configs.getString(ConfigDefine.CONNECTOR_CLASS);
        Class clazz = Class.forName(className);
        Connector connector = (Connector) clazz.newInstance();

        Long currentTimestamp = System.currentTimeMillis();
        configs.put(ConfigDefine.UPDATE_TIMESATMP, currentTimestamp);
        connector.start(configs);
        connectorKeyValueStore.put(connectorName, configs);
        List<KeyValue> taskConfigs = connector.taskConfigs();
        for(KeyValue keyValue : taskConfigs){
            keyValue.put(ConfigDefine.CONNECTOR_CLASS, connector.taskClass().getName());
            keyValue.put(ConfigDefine.OMS_DRIVER_URL, configs.getString(ConfigDefine.OMS_DRIVER_URL));
            keyValue.put(ConfigDefine.UPDATE_TIMESATMP, currentTimestamp);
        }
        putTaskConfigs(connectorName, taskConfigs);
        connector.stop();
        sendSynchronizeConfig();

        triggerListener();
    }

    @Override
    public void removeConnectorConfig(String connectorName) {

        KeyValue config = new ConnectKeyValue();
        config.put(ConfigDefine.UPDATE_TIMESATMP, System.currentTimeMillis());
        config.put(ConfigDefine.CONFIG_DELETED, 1);
        Map<String, KeyValue> connectorConfig = new HashMap<>();
        connectorConfig.put(connectorName, config);
        List<KeyValue> taskConfigList = new ArrayList<>();
        taskConfigList.add(config);

        connectorKeyValueStore.put(connectorName, config);
        putTaskConfigs(connectorName, taskConfigList);
        sendSynchronizeConfig();
    }

    @Override
    public Map<String, List<KeyValue>> getTaskConfigs() {

        Map<String, List<KeyValue>> result = new HashMap<>();
        Map<String, List<KeyValue>> taskConfigs = taskKeyValueStore.getKVMap();
        Map<String, KeyValue> filteredConnector = getConnectorConfigs();
        for(String connectorName : taskConfigs.keySet()){
            if(!filteredConnector.containsKey(connectorName)){
                continue;
            }
            result.put(connectorName, taskConfigs.get(connectorName));
        }
        return result;
    }

    private void putTaskConfigs(String connectorName, List<KeyValue> configs) {

        List<KeyValue> exist = taskKeyValueStore.get(connectorName);
        if(null != exist && exist.size() > 0){
            taskKeyValueStore.remove(connectorName);
        }
        taskKeyValueStore.put(connectorName, configs);
    }

    @Override
    public void persist() {

        this.connectorKeyValueStore.persist();
        this.taskKeyValueStore.persist();
    }

    @Override
    public void registerListener(ConnectorConfigUpdateListener listener) {

        this.connectorConfigUpdateListener.add(listener);
    }

    private void triggerListener(){

        if(null == this.connectorConfigUpdateListener){
            return;
        }
        for(ConnectorConfigUpdateListener listener : this.connectorConfigUpdateListener){
            listener.onConfigUpdate();
        }
    }

    private void sendOnlineConfig(){

        ConnAndTaskConfigs configs = new ConnAndTaskConfigs();
        configs.setConnectorConfigs(connectorKeyValueStore.getKVMap());
        configs.setTaskConfigs(taskKeyValueStore.getKVMap());
        dataSynchronizer.send(ConfigChangeEnum.ONLINE_KEY.name(), configs);
    }

    private void sendSynchronizeConfig(){

        ConnAndTaskConfigs configs = new ConnAndTaskConfigs();
        configs.setConnectorConfigs(connectorKeyValueStore.getKVMap());
        configs.setTaskConfigs(taskKeyValueStore.getKVMap());
        dataSynchronizer.send(ConfigChangeEnum.CONFIG_CHANG_KEY.name(), configs);
    }

    private class ConfigChangeCallback implements Callback<String, ConnAndTaskConfigs> {

        @Override
        public void onCompletion(Throwable error, String key, ConnAndTaskConfigs result) {

            boolean changed = false;
            switch (ConfigChangeEnum.valueOf(key)){
                case ONLINE_KEY:
                    mergeConfig(result);
                    changed = true;
                    sendSynchronizeConfig();
                    break;
                case CONFIG_CHANG_KEY:
                    changed = mergeConfig(result);
                    break;
                default:
                    break;
            }
            if(changed){
                triggerListener();
            }
        }
    }

    private boolean mergeConfig(ConnAndTaskConfigs newConnAndTaskConfig) {

        boolean changed = false;
        for(String connectorName : newConnAndTaskConfig.getConnectorConfigs().keySet()){
            KeyValue newConfig = newConnAndTaskConfig.getConnectorConfigs().get(connectorName);
            KeyValue oldConfig = getConnectorConfigs().get(connectorName);
            if(null == oldConfig){
                changed = true;
                connectorKeyValueStore.put(connectorName, newConfig);
                taskKeyValueStore.put(connectorName, newConnAndTaskConfig.getTaskConfigs().get(connectorName));
            }else{

                Long oldUpdateTime = oldConfig.getLong(ConfigDefine.UPDATE_TIMESATMP);
                Long newUpdateTime = newConfig.getLong(ConfigDefine.UPDATE_TIMESATMP);
                if(newUpdateTime > oldUpdateTime){
                    changed = true;
                    connectorKeyValueStore.put(connectorName, newConfig);
                    taskKeyValueStore.put(connectorName, newConnAndTaskConfig.getTaskConfigs().get(connectorName));
                }
            }
        }
        return changed;
    }

    private enum ConfigChangeEnum{

        /**
         * Insert or update config.
         */
        CONFIG_CHANG_KEY,

        /**
         * A worker online.
         */
        ONLINE_KEY
    }
}
