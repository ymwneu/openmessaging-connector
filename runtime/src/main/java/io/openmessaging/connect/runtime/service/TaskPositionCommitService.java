package io.openmessaging.connect.runtime.service;

import io.openmessaging.connect.runtime.common.LoggerName;
import io.openmessaging.connect.runtime.connectorwrapper.Worker;
import io.openmessaging.connect.runtime.utils.ServiceThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskPositionCommitService extends ServiceThread {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.OMS_RUNTIME);

    private Worker worker;
    public TaskPositionCommitService(Worker worker){
        this.worker = worker;
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            this.waitForRunning(10000);
            this.worker.commitTaskPosition();
        }

        log.info(this.getServiceName() + " service end");
    }

    @Override
    public String getServiceName() {
        return TaskPositionCommitService.class.getSimpleName();
    }
}
