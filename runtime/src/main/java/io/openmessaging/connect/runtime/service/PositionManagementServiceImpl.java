package io.openmessaging.connect.runtime.service;

import io.openmessaging.connect.runtime.store.MetaStore;
import io.openmessaging.connect.runtime.store.OffsetFileBasedStore;

public class OffsetManagementServiceImpl implements OffsetManagementService{

    private MetaStore metaStore;

    public OffsetManagementServiceImpl(){
        this.metaStore = new OffsetFileBasedStore();
    }


}
