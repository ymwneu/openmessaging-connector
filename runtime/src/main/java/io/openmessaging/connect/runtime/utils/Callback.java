package io.openmessaging.connect.runtime.utils;

public interface Callback<K, V> {

    void onCompletion(Throwable error, K key, V result);
}
