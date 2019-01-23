package io.openmessaging.connect.runtime.service;

import java.util.Set;

public interface ClusterManagementService {

    void start();

    void stop();

    Set<String> getAllAliveWorkers();
    interface WorkerStatusListener {

        void onWorkerChange();
    }
}
