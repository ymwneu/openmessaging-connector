package io.openmessaging.connect.runtime.utils.datasync;

public interface DataSynchronizerCallback<K, V> {

    void onCompletion(Throwable error, K key, V result);
}
