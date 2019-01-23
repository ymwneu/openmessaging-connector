package io.openmessaging.connect.runtime.store;

public interface MetaStore<T> {

    T getData();

    void setData(T data);

    boolean load();

    void persist();
}
