package io.openmessaging.connect.runtime.connectorwrapper.testimpl;

import io.openmessaging.connector.api.PositionStorageReader;

import java.util.Collection;
import java.util.Map;

public class TestPositionStorageReader implements PositionStorageReader {

    @Override
    public byte[] getPosition(byte[] partition) {
        return new byte[0];
    }

    @Override
    public Map<byte[], byte[]> getPositions(Collection<byte[]> partitions) {
        return null;
    }
}
