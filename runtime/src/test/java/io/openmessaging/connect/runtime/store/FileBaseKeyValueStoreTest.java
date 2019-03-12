package io.openmessaging.connect.runtime.store;

import io.openmessaging.connect.runtime.converter.ByteConverter;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.Test;

import java.util.HashMap;


public class FileBaseKeyValueStoreTest {

    @Test
    public void testFileBaseKeyValueStore() {
        FileBaseKeyValueStore<byte[], byte[]> fbkvs = new FileBaseKeyValueStore<>(
                "target/unit_test_store/testFileBaseKeyValueStore/000",
                new ByteConverter(),
                new ByteConverter()
        );

        fbkvs.data = new HashMap<>();
        fbkvs.data.put("test_key".getBytes(), "test_value".getBytes());
        fbkvs.persist();
        boolean flag = fbkvs.load();
        assertThat(flag).isEqualTo(true);
    }
}