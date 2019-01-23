package io.openmessaging.connect.runtime;

import io.openmessaging.KeyValue;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface AllocateConnAndTaskStrategy {

    ConnAndTaskConfigs allocate(Set<String> allWorker, String curWorker, Map<String, KeyValue> connectorConfigs,
        Map<String, List<KeyValue>> taskConfigs);
}
