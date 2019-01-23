package io.openmessaging.connector.api;

import java.util.Collection;
import java.util.Map;

public interface SourceTaskContext extends TaskContext {

    <T> Map<String, Object> getOffset(Map<String, T> queue);

    <T> Map<Map<String, T>, Map<String, Object>> getOffsets(Collection<Map<String, T>> queues);
}
