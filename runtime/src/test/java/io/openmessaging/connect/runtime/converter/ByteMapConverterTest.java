package io.openmessaging.connect.runtime.converter;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class ByteMapConverterTest {

    @Test
    public void testByteMapConverter() {
        Map<byte[], byte[]> data = new HashMap<>();
        data.put("test_key".getBytes(), "test_value".getBytes());
        ByteMapConverter bmc = new ByteMapConverter();
        byte[] o2b = bmc.objectToByte(data);
        Map<byte[], byte[]> b2o = bmc.byteToObject(o2b);
        assertThat(b2o.size()).isEqualTo(1);
        assertThat(b2o.keySet().size()).isEqualTo(1);
        assertThat(b2o.values().size()).isEqualTo(1);
        for (byte[] key: b2o.keySet()) {
            assertThat(new String(key)).isEqualTo("test_key");
            assertThat(new String(b2o.get(key))).isEqualTo("test_value");
        }
    }
}