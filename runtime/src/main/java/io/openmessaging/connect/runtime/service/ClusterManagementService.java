package io.openmessaging.connect.runtime.service;

import java.util.Set;

public interface ClusterManagementService {

    void start();

    void stop();

    Set<String> getAllAliveWorkers();

    void registerListener(WorkerStatusListener listener);

    interface WorkerStatusListener {

        void onWorkerChange();
    }
}
