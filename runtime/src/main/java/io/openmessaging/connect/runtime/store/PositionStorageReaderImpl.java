package io.openmessaging.connect.runtime.store;

import io.openmessaging.connector.api.source.OffsetStorageReader;
import java.util.Collection;
import java.util.Map;

public class OffsetStorageReaderImpl implements OffsetStorageReader {

    private MetaStore offsetMetaStore;

    public OffsetStorageReaderImpl(MetaStore offsetMetaStore){
        this.offsetMetaStore = offsetMetaStore;
    }

    @Override public <T> Map<String, Object> getPosition(Map<String, T> partition) {
        return null;
    }

    @Override public <T> Map<Map<String, T>, Map<String, Object>> getPositions(Collection<Map<String, T>> partitions) {
        return null;
    }
}
