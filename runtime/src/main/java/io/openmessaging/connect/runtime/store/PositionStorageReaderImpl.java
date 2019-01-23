package io.openmessaging.connect.runtime.store;

import io.openmessaging.connect.runtime.service.PositionManagementService;
import io.openmessaging.connector.api.PositionStorageReader;
import java.util.Collection;
import java.util.Map;

public class PositionStorageReaderImpl implements PositionStorageReader {

    private PositionManagementService positionManagementService;

    public PositionStorageReaderImpl(PositionManagementService positionManagementService){
        this.positionManagementService = positionManagementService;
    }

    @Override public <T> Map<String, ?> getPosition(Map<String, T> partition) {
        return positionManagementService.getPositionTable().get(partition);
    }

    @Override public <T> Map<Map<String, T>, Map<String, ?>> getPositions(Collection<Map<String, T>> partitions) {
        return null;
    }
}
