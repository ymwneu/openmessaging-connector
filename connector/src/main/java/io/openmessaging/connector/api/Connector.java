package io.openmessaging.connector.api;

import io.openmessaging.KeyValue;
import java.util.List;

public interface Connector {

    /**
     * Init the configuration of a connector. It will be invoke before {@link #start}.
     * @param config
     */
    void initConfiguration(KeyValue config);

    /**
     * Reconfigure while the connector is running.
     * @param config
     */
    void reconfigure(KeyValue config);

    /**
     * Start this connector.
     */
    void start();

    /**
     * stop this connector.
     */
    void stop();

    /**
     * Returns the Task implementation for this Connector.
     * @return
     */
    Class<? extends Task> taskClass();

    /**
     * Returns a set of configurations for Tasks based on the current configuration.
     * @return
     */
    List<KeyValue> taskConfigs();
}
