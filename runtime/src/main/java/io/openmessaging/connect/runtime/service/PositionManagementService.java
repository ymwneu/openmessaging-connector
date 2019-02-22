package io.openmessaging.connect.runtime.service;

import java.util.List;
import java.util.Map;

public interface PositionManagementService {

    void start();

    void stop();

    void persist();

    Map<byte[], byte[]> getPositionTable();

    void putPosition(Map<byte[], byte[]> positions);

    void removePosition(List<byte[]> partitions);

    void registerListener(PositionManagementService.PositionUpdateListener listener);

    interface PositionUpdateListener {
        void onPositionUpdate();
    }
}
