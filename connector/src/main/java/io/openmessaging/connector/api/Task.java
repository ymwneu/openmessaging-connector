package io.openmessaging.connector.api;

import io.openmessaging.KeyValue;

public interface Task {

    void start(KeyValue props);

    void stop();
}
