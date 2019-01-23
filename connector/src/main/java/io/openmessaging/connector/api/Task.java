package io.openmessaging.connector.api;

import java.util.Map;

public interface Task {

    void start(Map<String, String> props);

    void stop();

    Map<String, String> getCurrentConfigs();
}
