package io.openmessaging.connect.runtime.service;

public interface HeartBeatService {

    interface WorkerStatusListener {

        void onWorkerChange();
    }
}
