package io.openmessaging.connect.runtime.service;

import java.util.Map;

public interface OffsetManagementService {

    void start();

    void stop();

    void persist();

    Map<Map<String, ?>, Map<String, ?>> getPositionTable();

    void putPosition(Map<String, ?> partition, Map<String, ?> position);

    void removePosition(Map<String, ?> partition);
}
