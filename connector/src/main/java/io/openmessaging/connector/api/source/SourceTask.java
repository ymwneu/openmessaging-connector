package io.openmessaging.connector.api;

import java.util.Collection;

public abstract class SourceTask implements Task {

    protected SourceTaskContext context;

    public void initialize(SourceTaskContext context) {
        this.context = context;
    }

    /**
     * Return a collection of message to send.
     * */
    abstract Collection<ConnectRecord> poll();
}
