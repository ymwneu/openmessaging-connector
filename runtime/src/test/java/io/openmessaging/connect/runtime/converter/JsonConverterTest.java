package io.openmessaging.connect.runtime.converter;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class JsonConverterTest {

    @Test
    public void testJsonConverter() {
        Map<String, String> data = new HashMap<>();
        data.put("test_key", "test_value");
        JsonConverter jc = new JsonConverter();
        byte[] o2b = jc.objectToByte(data);
        Map<String, String> b2o = (Map<String, String>) jc.byteToObject(o2b);
        assertThat(b2o.size()).isEqualTo(1);
        assertThat(b2o.keySet().size()).isEqualTo(1);
        assertThat(b2o.values().size()).isEqualTo(1);
        for (String key: b2o.keySet()) {
            assertThat(new String(key)).isEqualTo("test_key");
            assertThat(new String(b2o.get(key))).isEqualTo("test_value");
        }
    }
}