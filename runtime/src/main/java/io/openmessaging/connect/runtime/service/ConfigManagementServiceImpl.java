package io.openmessaging.connect.runtime.service;

import io.openmessaging.KeyValue;
import io.openmessaging.MessagingAccessPoint;
import io.openmessaging.connect.runtime.common.ConnAndTaskConfigs;
import io.openmessaging.connect.runtime.common.ConnectKeyValue;
import io.openmessaging.connect.runtime.config.ConnectConfig;
import io.openmessaging.connect.runtime.config.RuntimeConfigDefine;
import io.openmessaging.connect.runtime.converter.ConnAndTaskConfigConverter;
import io.openmessaging.connect.runtime.converter.JsonConverter;
import io.openmessaging.connect.runtime.converter.ListConverter;
import io.openmessaging.connect.runtime.store.FileBaseKeyValueStore;
import io.openmessaging.connect.runtime.store.KeyValueStore;
import io.openmessaging.connect.runtime.utils.FilePathConfigUtil;
import io.openmessaging.connect.runtime.utils.datasync.BrokerBasedLog;
import io.openmessaging.connect.runtime.utils.datasync.DataSynchronizer;
import io.openmessaging.connect.runtime.utils.datasync.DataSynchronizerCallback;
import io.openmessaging.connector.api.Connector;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ConfigManagementServiceImpl implements ConfigManagementService {

    private static final String CONFIG_MESSAGE_TOPIC = "config-topic";

    private KeyValueStore<String, ConnectKeyValue> connectorKeyValueStore;
    private KeyValueStore<String, List<ConnectKeyValue>> taskKeyValueStore;
    private Set<ConnectorConfigUpdateListener> connectorConfigUpdateListener;
    private DataSynchronizer<String, ConnAndTaskConfigs> dataSynchronizer;

    public ConfigManagementServiceImpl(ConnectConfig connectConfig,
                                       MessagingAccessPoint messagingAccessPoint){

        this.connectorConfigUpdateListener = new HashSet<>();
        this.dataSynchronizer = new BrokerBasedLog<>(messagingAccessPoint,
                                                     CONFIG_MESSAGE_TOPIC,
                                                     connectConfig.getWorkerId()+System.currentTimeMillis(),
                                                     new ConfigManagementServiceImpl.ConfigChangeCallback(),
                                                     new JsonConverter(),
                                                     new ConnAndTaskConfigConverter());
        this.connectorKeyValueStore = new FileBaseKeyValueStore<>(
                                                     FilePathConfigUtil.getConnectorConfigPath(connectConfig.getStorePathRootDir()),
                                                     new JsonConverter(),
                                                     new JsonConverter(ConnectKeyValue.class));
        this.taskKeyValueStore = new FileBaseKeyValueStore<>(
                                                     FilePathConfigUtil.getTaskConfigPath(connectConfig.getStorePathRootDir()),
                                                     new JsonConverter(),
                                                     new ListConverter(ConnectKeyValue.class));
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
    public Map<String, ConnectKeyValue> getConnectorConfigs() {

        Map<String, ConnectKeyValue> result = new HashMap<>();
        Map<String, ConnectKeyValue> connectorConfigs = connectorKeyValueStore.getKVMap();
        for(String connectorName : connectorConfigs.keySet()){
            ConnectKeyValue config = connectorConfigs.get(connectorName);
            if(0 != config.getInt(RuntimeConfigDefine.CONFIG_DELETED)){
                continue;
            }
            result.put(connectorName, config);
        }
        return result;
    }

    @Override
    public String putConnectorConfig(String connectorName, ConnectKeyValue configs) throws Exception {

        ConnectKeyValue exist = connectorKeyValueStore.get(connectorName);
        if(configs.equals(exist)){
            return "";
        }
        String className = configs.getString(RuntimeConfigDefine.CONNECTOR_CLASS);
        Class clazz = Class.forName(className);
        Connector connector = (Connector) clazz.newInstance();

        Long currentTimestamp = System.currentTimeMillis();
        configs.put(RuntimeConfigDefine.UPDATE_TIMESATMP, currentTimestamp);
        for(String requireConfig : RuntimeConfigDefine.REQUEST_CONFIG){
            if(!configs.containsKey(requireConfig)){
                return "Request config key: " + requireConfig;
            }
        }
        String errorMessage = connector.verifyAndSetConfig(configs);
        if(errorMessage != null && errorMessage.length() > 0){
            return errorMessage;
        }
        connector.start();
        connectorKeyValueStore.put(connectorName, configs);
        List<KeyValue> taskConfigs = connector.taskConfigs();
        List<ConnectKeyValue> converterdConfigs = new ArrayList<>();
        for(KeyValue keyValue : taskConfigs){
            ConnectKeyValue newKeyValue = new ConnectKeyValue();
            for(String key : keyValue.keySet()){
                newKeyValue.put(key, keyValue.getString(key));
            }
            newKeyValue.put(RuntimeConfigDefine.CONNECTOR_CLASS, connector.taskClass().getName());
            newKeyValue.put(RuntimeConfigDefine.OMS_DRIVER_URL, configs.getString(RuntimeConfigDefine.OMS_DRIVER_URL));
            newKeyValue.put(RuntimeConfigDefine.UPDATE_TIMESATMP, currentTimestamp);
            converterdConfigs.add(newKeyValue);
        }
        putTaskConfigs(connectorName, converterdConfigs);
        connector.stop();
        sendSynchronizeConfig();

        triggerListener();
        return "";
    }

    @Override
    public void removeConnectorConfig(String connectorName) {

        ConnectKeyValue config = new ConnectKeyValue();
        config.put(RuntimeConfigDefine.UPDATE_TIMESATMP, System.currentTimeMillis());
        config.put(RuntimeConfigDefine.CONFIG_DELETED, 1);
        Map<String, ConnectKeyValue> connectorConfig = new HashMap<>();
        connectorConfig.put(connectorName, config);
        List<ConnectKeyValue> taskConfigList = new ArrayList<>();
        taskConfigList.add(config);

        connectorKeyValueStore.put(connectorName, config);
        putTaskConfigs(connectorName, taskConfigList);
        sendSynchronizeConfig();
    }

    @Override
    public Map<String, List<ConnectKeyValue>> getTaskConfigs() {

        Map<String, List<ConnectKeyValue>> result = new HashMap<>();
        Map<String, List<ConnectKeyValue>> taskConfigs = taskKeyValueStore.getKVMap();
        Map<String, ConnectKeyValue> filteredConnector = getConnectorConfigs();
        for(String connectorName : taskConfigs.keySet()){
            if(!filteredConnector.containsKey(connectorName)){
                continue;
            }
            result.put(connectorName, taskConfigs.get(connectorName));
        }
        return result;
    }

    private void putTaskConfigs(String connectorName, List<ConnectKeyValue> configs) {

        List<ConnectKeyValue> exist = taskKeyValueStore.get(connectorName);
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

    private class ConfigChangeCallback implements DataSynchronizerCallback<String, ConnAndTaskConfigs> {

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
            ConnectKeyValue newConfig = newConnAndTaskConfig.getConnectorConfigs().get(connectorName);
            ConnectKeyValue oldConfig = getConnectorConfigs().get(connectorName);
            if(null == oldConfig){

                changed = true;
                connectorKeyValueStore.put(connectorName, newConfig);
                taskKeyValueStore.put(connectorName, newConnAndTaskConfig.getTaskConfigs().get(connectorName));
            }else{

                Long oldUpdateTime = oldConfig.getLong(RuntimeConfigDefine.UPDATE_TIMESATMP);
                Long newUpdateTime = newConfig.getLong(RuntimeConfigDefine.UPDATE_TIMESATMP);
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
