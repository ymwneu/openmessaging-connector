package io.openmessaging.connector.api.sink;

public class Partition {

    /**
     *
     */
    private String queueName;
    private String key;

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }
}
