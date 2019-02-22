package io.openmessaging.connect.runtime.converter;

import io.openmessaging.connector.api.data.Converter;

public class ByteConverter implements Converter<byte[]> {

    @Override
    public byte[] objectToByte(byte[] object) {
        return object;
    }

    @Override
    public byte[] byteToObject(byte[] bytes) {
        return bytes;
    }
}
