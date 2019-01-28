package io.openmessaging.connect.runtime.utils;

public interface DataSynchronizer<K, V> {

    void start();

    void stop();

    void send(K key, V value);
}
