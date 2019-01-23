package io.openmessaging.connect.runtime.store;

import io.openmessaging.consumer.MessageListener;
import io.openmessaging.producer.Producer;

public class BrokerBasedStore {

    private Producer producer;
//    private Consumer consumer;
    private MessageListener messageListener;

    public BrokerBasedStore(MessageListener messageListener){
        this.messageListener = messageListener;
    }

    public void start(){

//        consumer.bindQueue(simpleQueue, this.messageListener);
    }

    public void send(String key, String value){

    }
}
