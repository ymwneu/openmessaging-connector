package io.openmessaging.connect.runtime.converter;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;


public class ByteConverterTest {

    @Test
    public void testByteConverter() {
        String test = "test";
        ByteConverter bc = new ByteConverter();
        byte[] b2o = bc.byteToObject(test.getBytes());
        byte[] o2b = bc.objectToByte(b2o);

        assertThat(new String(o2b)).isEqualTo("test");
    }
}