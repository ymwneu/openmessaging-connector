package io.openmessaging.connect.runtime.service;

import io.openmessaging.MessagingAccessPoint;
import io.openmessaging.connect.runtime.config.ConnectConfig;
import io.openmessaging.connect.runtime.converter.ByteConverter;
import io.openmessaging.connect.runtime.converter.ByteMapConverter;
import io.openmessaging.connect.runtime.converter.JsonConverter;
import io.openmessaging.connect.runtime.store.FileBaseKeyValueStore;
import io.openmessaging.connect.runtime.store.KeyValueStore;
import io.openmessaging.connect.runtime.utils.FilePathConfigUtil;
import io.openmessaging.connect.runtime.utils.datasync.BrokerBasedLog;
import io.openmessaging.connect.runtime.utils.datasync.DataSynchronizer;
import io.openmessaging.connect.runtime.utils.datasync.DataSynchronizerCallback;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PositionManagementServiceImpl implements PositionManagementService {

    private static final String POSITION_MESSAGE_TOPIC = "position-topic";

    private KeyValueStore<byte[], byte[]> positionStore;
    private DataSynchronizer<String, Map<byte[], byte[]>> dataSynchronizer;
    private Set<PositionUpdateListener> positionUpdateListener;

    public PositionManagementServiceImpl(ConnectConfig connectConfig,
                                         MessagingAccessPoint messagingAccessPoint){

        this.positionStore = new FileBaseKeyValueStore<>(FilePathConfigUtil.getPositionPath(connectConfig.getStorePathRootDir()),
                                                         new ByteConverter(),
                                                         new ByteConverter());
        this.dataSynchronizer = new BrokerBasedLog(messagingAccessPoint,
                                                    POSITION_MESSAGE_TOPIC,
                                                    connectConfig.getWorkerId()+System.currentTimeMillis(),
                                                    new PositionManagementServiceImpl.PositionChangeCallback(),
                                                    new JsonConverter(),
                                                    new ByteMapConverter());
        this.positionUpdateListener = new HashSet<>();
    }

    @Override
    public void start() {

        positionStore.load();
        dataSynchronizer.start();
        sendOnlinePositionInfo();
    }

    @Override
    public void stop() {

        positionStore.persist();
        dataSynchronizer.stop();
    }

    @Override
    public void persist() {

        positionStore.persist();
    }

    @Override
    public Map<byte[], byte[]> getPositionTable() {
        return positionStore.getKVMap();
    }

    @Override
    public void putPosition(Map<byte[], byte[]> positions) {

        positionStore.putAll(positions);
        sendSynchronizePosition();
    }

    @Override
    public void removePosition(List<byte[]> partitions) {

        if(null == partitions){
             return;
        }
        for(byte[] partition : partitions){
            positionStore.remove(partition);
        }
    }

    @Override
    public void registerListener(PositionUpdateListener listener) {

        this.positionUpdateListener.add(listener);
    }

    private void sendOnlinePositionInfo() {

        dataSynchronizer.send(PositionChangeEnum.ONLINE_KEY.name(), positionStore.getKVMap());
    }

    private void sendSynchronizePosition(){

        dataSynchronizer.send(PositionChangeEnum.POSITION_CHANG_KEY.name(), positionStore.getKVMap());
    }

    private class PositionChangeCallback implements DataSynchronizerCallback<String, Map<byte[], byte[]>> {

        @Override
        public void onCompletion(Throwable error, String key, Map<byte[], byte[]> result) {
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

    private boolean mergePositionInfo(Map<byte[], byte[]> result) {

        boolean changed = false;
        if(null == result || 0 == result.size()){
            return changed;
        }

        for(Map.Entry<byte[], byte[]> newEntry : result.entrySet()){
            boolean find = false;
            for(Map.Entry<byte[], byte[]> existedEntry : positionStore.getKVMap().entrySet()){
                if(Arrays.equals(newEntry.getKey(), existedEntry.getKey())){
                    find = true;
                    if(!Arrays.equals(newEntry.getValue(), existedEntry.getValue())){
                        changed = true;
                        existedEntry.setValue(newEntry.getValue());
                    }
                    break;
                }
            }
            if(!find){
                positionStore.put(newEntry.getKey(), newEntry.getValue());
            }
        }
        return changed;
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

