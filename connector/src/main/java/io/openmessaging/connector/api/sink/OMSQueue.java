package io.openmessaging.connector.api.sink;

public class OMSQueue {

    /**
     * namespace of a queue
     */
    private String namespace;

    /**
     * target queue to send/fetch message
     */
    private String queueName;

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }
}
