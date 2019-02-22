package io.openmessaging.connect.runtime.service.strategy;

import io.openmessaging.connect.runtime.common.ConnAndTaskConfigs;
import io.openmessaging.connect.runtime.common.ConnectKeyValue;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface AllocateConnAndTaskStrategy {

    ConnAndTaskConfigs allocate(Set<String> allWorker, String curWorker, Map<String, ConnectKeyValue> connectorConfigs,
        Map<String, List<ConnectKeyValue>> taskConfigs);
}
