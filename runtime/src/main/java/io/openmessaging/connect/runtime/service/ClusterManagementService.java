package io.openmessaging.connect.runtime.service;

import java.util.Map;

public interface ClusterManagementService {

    void start();

    void stop();

    Map<String, Long> getAllAliveWorkers();

    void registerListener(WorkerStatusListener listener);

    interface WorkerStatusListener {

        void onWorkerChange();
    }
}
