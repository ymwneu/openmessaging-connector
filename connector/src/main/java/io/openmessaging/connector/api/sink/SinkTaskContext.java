package io.openmessaging.connector.api;

import java.util.Map;
import java.util.Set;

public interface SinkTaskContext extends TaskContext {

    void resetOffset(String queueName, Long offset);

    void resetOffset(Map<String, Long> offsets);

    Set<String> getAssignedQueueSet();

    // pause, resume
}
