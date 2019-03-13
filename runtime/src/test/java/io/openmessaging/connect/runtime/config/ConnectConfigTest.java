package io.openmessaging.connect.runtime.config;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ConnectConfigTest {

    @Test
    public void testConnectConfigAttribute() {
        ConnectConfig connectConfig = new ConnectConfig();
        connectConfig.setHttpPort(8081);
        connectConfig.setOmsDriverUrl("oms:rocketmq://localhost:9876/default:default");
        connectConfig.setWorkerId("DEFAULT_WORKER_1");
        assertThat(connectConfig.getHttpPort()).isEqualTo(8081);
        assertThat(connectConfig.getOmsDriverUrl()).isEqualTo("oms:rocketmq://localhost:9876/default:default");
        assertThat(connectConfig.getWorkerId()).isEqualTo("DEFAULT_WORKER_1");
    }
}