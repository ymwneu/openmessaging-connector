package io.openmessaging.connect.runtime.connectorwrapper.testimpl;

import io.openmessaging.connector.api.data.Converter;

public class TestConverter implements Converter {

    @Override
    public byte[] objectToByte(Object object) {
        return "test-converter".getBytes();
    }

    @Override
    public Object byteToObject(byte[] bytes) {
        return null;
    }
}
