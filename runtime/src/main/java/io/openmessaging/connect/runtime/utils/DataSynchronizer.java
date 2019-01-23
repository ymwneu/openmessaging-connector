package io.openmessaging.connect.runtime.utils;

public interface DataSynchronizer<K, V> {

    void start(Callback<K, V> callback);

    void stop();

    void send(K key, V value);
}
