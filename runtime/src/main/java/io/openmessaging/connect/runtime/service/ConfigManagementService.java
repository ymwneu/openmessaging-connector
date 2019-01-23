package io.openmessaging.connect.runtime.store;

public interface ConfigBackingStore {

    interface ConfigUpdateListener{
        void onConfigUpdate();
    }
}
