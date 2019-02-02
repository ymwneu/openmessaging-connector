package io.openmessaging.connect.runtime.store;

import java.util.HashMap;
import java.util.Map;

public class MemoryBasedKeyValueStore<K, V> implements KeyValueStore<K, V> {

    protected Map<K, V> data;

    public MemoryBasedKeyValueStore(){
        this.data = new HashMap<>();
    }

    @Override
    public V put(K key, V value) {
        return this.data.put(key, value);
    }

    @Override public void putAll(Map<K, V> map) {
        data.putAll(map);
    }

    @Override
    public V remove(K key) {
        return this.data.remove(key);
    }

    @Override
    public int size() {
        return this.data.size();
    }

    @Override
    public boolean contansKey(K key) {
        return this.data.containsKey(key);
    }

    @Override
    public V get(K key) {
        return this.data.get(key);
    }

    @Override
    public Map<K, V> getKVMap() {
        return this.data;
    }

    @Override
    public boolean load() {
        return true;
    }

    @Override
    public void persist() {

    }
}
