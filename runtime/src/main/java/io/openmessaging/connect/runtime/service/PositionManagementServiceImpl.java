package io.openmessaging.connect.runtime.service;

import io.openmessaging.MessagingAccessPoint;
import io.openmessaging.connect.runtime.config.ConnectConfig;
import io.openmessaging.connect.runtime.store.FileBaseKeyValueBasedKeyValueStore;
import io.openmessaging.connect.runtime.store.KeyValueStore;
import io.openmessaging.connect.runtime.utils.BrokerBasedLog;
import io.openmessaging.connect.runtime.utils.Callback;
import io.openmessaging.connect.runtime.utils.DataSynchronizer;
import io.openmessaging.connect.runtime.utils.FilePathConfigUtil;
import io.openmessaging.connect.runtime.utils.JsonConverter;
import io.openmessaging.connector.api.sink.OMSQueue;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PositionManagementServiceImpl implements PositionManagementService {

    private static final OMSQueue POSITION_MESSAGE_TOPIC = new OMSQueue("position-topic", 0);

    private KeyValueStore<Map<String, ?>, Map<String, ?>> positionStore;
    private DataSynchronizer<String, Map<Map<String, ?>, Map<String, ?>>> dataSynchronizer;
    private Set<PositionUpdateListener> positionUpdateListener;

    public PositionManagementServiceImpl(ConnectConfig connectConfig,
                                         MessagingAccessPoint messagingAccessPoint){

        this.positionStore = new FileBaseKeyValueBasedKeyValueStore<>(FilePathConfigUtil.getPositionPath(connectConfig.getStorePathRootDir()));
        this.dataSynchronizer = new BrokerBasedLog(messagingAccessPoint,
                                                    POSITION_MESSAGE_TOPIC,
                                                    connectConfig.getWorkerId()+System.currentTimeMillis(),
                                                    new PositionManagementServiceImpl.PositionChangeCallback(),
                                                    new JsonConverter(),
                                                    new JsonConverter(),
                                                    String.class,
                                                    Map.class);
        this.positionUpdateListener = new HashSet<>();
    }

    @Override public void start() {

        positionStore.load();
        dataSynchronizer.start();
        sendOnlinePositionInfo();
    }

    @Override public void stop() {

        positionStore.persist();
        dataSynchronizer.stop();
    }

    @Override public void persist() {

        positionStore.persist();
    }

    @Override public Map<Map<String, ?>, Map<String, ?>> getPositionTable() {
        return positionStore.getKVMap();
    }

    @Override public void putPosition(Map<Map<String, ?>, Map<String, ?>> positions) {

        positionStore.putAll(positions);
    }

    @Override public void removePosition(List<Map<String, ?>> partitions) {

        if(null == partitions){
             return;
        }
        for(Map<String, ?> partition : partitions){
            positionStore.remove(partition);
        }
    }

    @Override public void registerListener(PositionUpdateListener listener) {

        this.positionUpdateListener.add(listener);
    }

    private void sendOnlinePositionInfo() {

        dataSynchronizer.send(PositionChangeEnum.ONLINE_KEY.name(), positionStore.getKVMap());
    }

    private void sendSynchronizePosition(){

        dataSynchronizer.send(PositionChangeEnum.POSITION_CHANG_KEY.name(), positionStore.getKVMap());
    }

    private class PositionChangeCallback implements Callback<String, Map<Map<String, ?>, Map<String, ?>>> {

        @Override public void onCompletion(Throwable error, String key, Map<Map<String, ?>, Map<String, ?>> result) {
            // update positionStore
            PositionManagementServiceImpl.this.persist();

            boolean changed = false;
            switch (PositionManagementServiceImpl.PositionChangeEnum.valueOf(key)){
                case ONLINE_KEY:
                    mergePositionInfo(result);
                    changed = true;
                    sendSynchronizePosition();
                    break;
                case POSITION_CHANG_KEY:
                    changed = mergePositionInfo(result);
                    break;
                default:
                    break;
            }
            if(changed){
                triggerListener();
            }

        }
    }

    private void triggerListener() {
        for(PositionUpdateListener positionUpdateListener : positionUpdateListener){
            positionUpdateListener.onPositionUpdate();
        }
    }

    private boolean mergePositionInfo(Map<Map<String, ?>, Map<String, ?>> result) {

        return false;
    }

    private enum PositionChangeEnum{

        /**
         * Insert or update position info.
         */
        POSITION_CHANG_KEY,

        /**
         * A worker online.
         */
        ONLINE_KEY
    }
}

