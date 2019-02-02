package io.openmessaging.connect.runtime.utils;

import io.openmessaging.KeyValue;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ConnectKeyValue implements KeyValue, Serializable {

    private Map<String, String> properties;

    public ConnectKeyValue() {
        properties = new ConcurrentHashMap<String, String>();
    }

    @Override
    public KeyValue put(String key, int value) {
        properties.put(key, String.valueOf(value));
        return this;
    }

    @Override
    public KeyValue put(String key, long value) {
        properties.put(key, String.valueOf(value));
        return this;
    }

    @Override
    public KeyValue put(String key, double value) {
        properties.put(key, String.valueOf(value));
        return this;
    }

    @Override
    public KeyValue put(String key, String value) {
        properties.put(key, String.valueOf(value));
        return this;
    }

    @Override
    public int getInt(String key) {
        if (!properties.containsKey(key))
            return 0;
        return Integer.valueOf(properties.get(key));
    }

    @Override
    public int getInt(final String key, final int defaultValue) {
        return properties.containsKey(key) ? getInt(key) : defaultValue;
    }

    @Override
    public long getLong(String key) {
        if (!properties.containsKey(key))
            return 0;
        return Long.valueOf(properties.get(key));
    }

    @Override
    public long getLong(final String key, final long defaultValue) {
        return properties.containsKey(key) ? getLong(key) : defaultValue;
    }

    @Override
    public double getDouble(String key) {
        if (!properties.containsKey(key))
            return 0;
        return Double.valueOf(properties.get(key));
    }

    @Override
    public double getDouble(final String key, final double defaultValue) {
        return properties.containsKey(key) ? getDouble(key) : defaultValue;
    }

    @Override
    public String getString(String key) {
        return properties.get(key);
    }

    @Override
    public String getString(final String key, final String defaultValue) {
        return properties.containsKey(key) ? getString(key) : defaultValue;
    }

    @Override
    public Set<String> keySet() {
        return properties.keySet();
    }

    @Override
    public boolean containsKey(String key) {
        return properties.containsKey(key);
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }
}
