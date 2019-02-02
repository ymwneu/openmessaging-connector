package io.openmessaging.connect.runtime;

import io.openmessaging.connect.runtime.common.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskPositionCommitService extends ServiceThread{

    private static final Logger log = LoggerFactory.getLogger(LoggerName.OMS_RUNTIME);

    private Worker worker;
    public TaskPositionCommitService(Worker worker){
        this.worker = worker;
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            this.waitForRunning(10);
            this.worker.commitTaskPosition();
        }

        log.info(this.getServiceName() + " service end");
    }

    @Override
    public String getServiceName() {
        return TaskPositionCommitService.class.getSimpleName();
    }
}
