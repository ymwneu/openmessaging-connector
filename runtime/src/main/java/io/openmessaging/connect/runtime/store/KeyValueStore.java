package io.openmessaging.connect.runtime.store;

import java.util.Map;

public interface KeyValueStore<K, V> {

    V put(K key, V value);

    void putAll(Map<K, V> map);

    V remove(K key);

    int size();

    boolean contansKey(K key);

    V get(K key);

    Map<K, V> getKVMap();

    boolean load();

    void persist();
}
