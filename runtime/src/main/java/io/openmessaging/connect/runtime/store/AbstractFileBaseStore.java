package io.openmessaging.connect.runtime.store;

public abstract class AbstractFileBaseStore<T> implements MetaStore<T>{

    public abstract String encode();

    public abstract void decode(final String jsonString);

    public abstract String configFilePath();

    @Override
    public boolean load() {
        return false;
    }

    @Override
    public void persist() {

    }
}
