package io.openmessaging.connect.runtime.store;

import java.util.HashMap;
import java.util.Map;

public class OffsetStorageWriter {

    private MetaStore offsetMetaStore;
    private Map<Map<String, ?>, Map<String, ?>> data = new HashMap<>();

    public OffsetStorageWriter(MetaStore offsetMetaStore){
        this.offsetMetaStore = offsetMetaStore;
    }

    public void recordOffset(Map<String, ?> partition, Map<String, ?> offset){
        this.data.put(partition, offset);
    }

    public void flush(){
        offsetMetaStore.persist();
    }
}
