package io.openmessaging.connect.runtime;

public class TaskPositionCommitService extends ServiceThread{

    private Worker worker;
    public TaskPositionCommitService(Worker worker){
        this.worker = worker;
    }

    @Override
    public void run() {
//        log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            this.waitForRunning(10);
            this.worker.commitTaskPosition();
        }

//        log.info(this.getServiceName() + " service end");
    }

    @Override
    public String getServiceName() {
        return TaskPositionCommitService.class.getSimpleName();
    }
}
