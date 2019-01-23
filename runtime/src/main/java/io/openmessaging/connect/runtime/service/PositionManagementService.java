package io.openmessaging.connect.runtime.service;

import java.util.List;
import java.util.Map;

public interface PositionManagementService {

    void start();

    void stop();

    void persist();

    Map<Map<String, ?>, Map<String, ?>> getPositionTable();

    void putPosition(Map<Map<String, ?>, Map<String, ?>> positions);

    void removePosition(List<Map<String, ?>> partitions);
}
