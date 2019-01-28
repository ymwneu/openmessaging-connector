package io.openmessaging.connect.runtime.store;

public class MemoryStore<T> implements MetaStore<T> {

    private T data;

    @Override public T getData() {
        return this.data;
    }

    @Override public void setData(T data) {
        this.data = data;
    }

    @Override public boolean load() {
        return true;
    }

    @Override public void persist() {

    }
}
