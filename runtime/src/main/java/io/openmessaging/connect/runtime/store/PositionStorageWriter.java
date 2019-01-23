package io.openmessaging.connect.runtime.store;

import io.openmessaging.connect.runtime.service.PositionManagementService;
import java.util.HashMap;
import java.util.Map;

public class PositionStorageWriter {

    private PositionManagementService positionManagementService;
    private Map<Map<String, ?>, Map<String, ?>> data = new HashMap<>();

    public PositionStorageWriter(PositionManagementService positionManagementService){
        this.positionManagementService = positionManagementService;
    }

    public void recordOffset(Map<String, ?> partition, Map<String, ?> offset){
        this.data.put(partition, offset);
    }

    public void flush(){
        positionManagementService.putPosition(data);
    }
}
