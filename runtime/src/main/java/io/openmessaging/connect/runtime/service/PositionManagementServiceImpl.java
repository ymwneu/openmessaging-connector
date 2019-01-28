package io.openmessaging.connect.runtime.service;

import io.openmessaging.MessagingAccessPoint;
import io.openmessaging.connect.runtime.store.MetaStore;
import io.openmessaging.connect.runtime.store.PositionFileBasedStore;
import io.openmessaging.connect.runtime.utils.BrokerBasedLog;
import io.openmessaging.connect.runtime.utils.Callback;
import io.openmessaging.connect.runtime.utils.DataSynchronizer;
import java.util.List;
import java.util.Map;

public class PositionManagementServiceImpl implements PositionManagementService {

    private MetaStore metaStore;
    private DataSynchronizer<Map<String, ?>, Map<String, ?>> dataSynchronizer;

    public PositionManagementServiceImpl(MessagingAccessPoint point){
        this.metaStore = new PositionFileBasedStore();
        this.dataSynchronizer = null;
    }

    @Override public void start() {
        dataSynchronizer.start();
    }

    @Override public void stop() {

    }

    @Override public void persist() {

    }

    @Override public Map<Map<String, ?>, Map<String, ?>> getPositionTable() {
        return null;
    }

    @Override public void putPosition(Map<Map<String, ?>, Map<String, ?>> positions) {

    }

    @Override public void removePosition(List<Map<String, ?>> partitions) {

    }

    private class PositionChangeCallback implements Callback<Map<String, ?>, Map<String, ?>> {

        @Override public void onCompletion(Throwable error, Map<String, ?> key, Map<String, ?> result) {
            // update metaStore
            PositionManagementServiceImpl.this.persist();
        }
    }
}
