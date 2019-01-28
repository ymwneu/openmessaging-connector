package io.openmessaging.connect.runtime.utils;

import com.alibaba.fastjson.JSON;
import io.openmessaging.Future;
import io.openmessaging.FutureListener;
import io.openmessaging.Message;
import io.openmessaging.MessagingAccessPoint;
import io.openmessaging.OMS;
import io.openmessaging.OMSBuiltinKeys;
import io.openmessaging.connector.api.sink.OMSQueue;
import io.openmessaging.consumer.MessageListener;
import io.openmessaging.consumer.PushConsumer;
import io.openmessaging.producer.Producer;
import io.openmessaging.producer.SendResult;
import java.util.HashMap;
import java.util.Map;

public class BrokerBasedLog<K, V> implements  DataSynchronizer<K, V>{

    private Callback<K, V> callback;
    private MessagingAccessPoint messagingAccessPoint;
    private Producer producer;
    private PushConsumer consumer;
    private OMSQueue omsQueue;
    private Converter converter;

    public BrokerBasedLog(MessagingAccessPoint messagingAccessPoint,
                          OMSQueue omsQueue,
                          String consumerId,
                          Callback<K, V> callback){
        this.messagingAccessPoint = messagingAccessPoint;
        this.omsQueue = omsQueue;
        this.callback = callback;
        producer = messagingAccessPoint.createProducer();
        consumer = messagingAccessPoint.createPushConsumer(
                                            OMS.newKeyValue().put(OMSBuiltinKeys.CONSUMER_ID, consumerId));
        converter = new BasicConverter();
    }
    @Override public void start() {

        producer.startup();

        consumer.attachQueue(omsQueue.getQueue(), new MessageListener() {
            @Override
            public void onReceived(Message message, Context context) {
                System.out.printf("Received one message: %s%n", message.sysHeaders().getString(Message.BuiltinKeys.MESSAGE_ID));
                byte[] bytes = message.getBody(byte[].class);
                Map<K, V> map = decodeKeyValue(bytes);
                for(K key : map.keySet()){
                    callback.onCompletion(null, key, map.get(key));
                }
                context.ack();
            }
        });
        consumer.startup();
    }

    @Override
    public void stop(){

    }

    @Override
    public void send(K key, V value){

        Future<SendResult> result = producer.sendAsync(producer.createBytesMessage(omsQueue.getQueue(), encodeKeyValue(key, value)));
        result.addListener(new FutureListener<SendResult>() {
            @Override
            public void operationComplete(Future<SendResult> future) {
                if (future.getThrowable() != null) {
                    System.out.printf("Send async message Failed, error: %s%n", future.getThrowable().getMessage());
                } else {
                    System.out.printf("Send async message OK, msgId: %s%n", future.get().messageId());
                }
            }
        });
    }

    private byte[] encodeKeyValue(K key, V value){

        Map<K, V> map = new HashMap<>();
        map.put(key, value);
        return converter.objectToByte(map);
    }

    private Map<K, V> decodeKeyValue(byte[] bytes){
        Map<K, V> map = (Map<K, V>) converter.byteToObject(bytes);
        return map;
    }

}
