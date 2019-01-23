package io.openmessaging.connector.api;

import java.util.Collection;

public abstract class SinkTask implements Task{

    protected SinkTaskContext context;

    public void initialize(SinkTaskContext context) {
        this.context = context;
    }

    /**
     * put the messages in the sink.
     * */
    abstract void put(Collection<ConnectRecord> message);
}
