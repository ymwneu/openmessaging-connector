package io.openmessaging.connect.runtime.utils;

public interface Converter {

    byte[] objectToByte(Object object);
    Object byteToObject(byte[] bytes);
}
