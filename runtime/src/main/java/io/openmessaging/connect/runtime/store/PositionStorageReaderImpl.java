package io.openmessaging.connect.runtime.store;

import io.openmessaging.connect.runtime.service.PositionManagementService;
import io.openmessaging.connector.api.PositionStorageReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class PositionStorageReaderImpl implements PositionStorageReader {

    private PositionManagementService positionManagementService;

    public PositionStorageReaderImpl(PositionManagementService positionManagementService){
        this.positionManagementService = positionManagementService;
    }

    @Override
    public byte[] getPosition(byte[] partition) {
        Map<byte[], byte[]> allData = positionManagementService.getPositionTable();

        for(Map.Entry<byte[], byte[]> entry : allData.entrySet()){
            if(Arrays.equals(entry.getKey(), partition)){
                return entry.getValue();
            }
        }
        return null;
    }

    @Override
    public Map<byte[], byte[]> getPositions(Collection<byte[]> partitions) {
        Map<byte[], byte[]> result = new HashMap<>();
        Map<byte[], byte[]> allData = positionManagementService.getPositionTable();
        for(Map.Entry<byte[], byte[]> entry : allData.entrySet()){
            for(byte[] partition : partitions){
                if(Arrays.equals(entry.getKey(), partition)){
                    result.put(entry.getKey(), entry.getValue());
                }
            }
        }
        return result;
    }
}
