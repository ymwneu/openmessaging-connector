package io.openmessaging.connector.api.sink;

import io.openmessaging.connector.api.data.DataEntry;

public class SinkDataEntry extends DataEntry {

    /**
     * offset in the queue.
     */
    private Long queueOffset;

    public Long getQueueOffset() {
        return queueOffset;
    }

    public void setQueueOffset(Long queueOffset) {
        this.queueOffset = queueOffset;
    }
}
