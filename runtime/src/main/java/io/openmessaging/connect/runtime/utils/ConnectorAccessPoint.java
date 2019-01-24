package io.openmessaging.connect.runtime.utils;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.Connector;
import io.openmessaging.connector.api.Task;

public interface ConnectorAccessPoint {

    Connector createConnector(String connectorName);

    Task createTask(String connectorName, KeyValue keyValue);
}
