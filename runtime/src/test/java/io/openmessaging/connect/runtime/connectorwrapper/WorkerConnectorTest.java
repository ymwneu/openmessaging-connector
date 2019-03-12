package io.openmessaging.connect.runtime.connectorwrapper;

import io.openmessaging.connect.runtime.common.ConnectKeyValue;
import io.openmessaging.connect.runtime.connectorwrapper.testimpl.TestConnector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class WorkerConnectorTest {

    private WorkerConnector workerConnector;

    @Before
    public void init() {
        ConnectKeyValue connectKeyValue = new ConnectKeyValue();
        connectKeyValue.getProperties().put("key1", "value1");
        workerConnector = new WorkerConnector("TEST-CONN", new TestConnector(), connectKeyValue);
        workerConnector.start();
    }

    @After
    public void destroy() {
        workerConnector.stop();
    }

    @Test
    public void testReconfigure() {
        ConnectKeyValue connectKeyValue = new ConnectKeyValue();
        connectKeyValue.getProperties().put("test2", "value2");
        workerConnector.reconfigure(connectKeyValue);
        assertThat(workerConnector.getKeyValue().equals(connectKeyValue)).isEqualTo(true);
    }

}