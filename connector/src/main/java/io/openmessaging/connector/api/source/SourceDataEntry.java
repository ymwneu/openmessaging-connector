package io.openmessaging.connector.api.source;

import io.openmessaging.connector.api.data.DataEntry;
import java.util.Map;

public class SourceDataEntry extends DataEntry {

    /**
     * Partition of the data source.
     */
    private Map<String, ?> sourcePartition;

    /**
     * position of current data entry in data source
     */
    private Map<String, ?> sourcePosition;

    public Map<String, ?> getSourcePartition() {
        return sourcePartition;
    }

    public void setSourcePartition(Map<String, ?> sourcePartition) {
        this.sourcePartition = sourcePartition;
    }

    public Map<String, ?> getSourcePosition() {
        return sourcePosition;
    }

    public void setSourcePosition(Map<String, ?> sourcePosition) {
        this.sourcePosition = sourcePosition;
    }
}
