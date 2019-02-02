package io.openmessaging.connect.runtime.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
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
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class BrokerBasedLog<K, V> implements DataSynchronizer<K, V>{

    private Callback<K, V> callback;
    private Producer producer;
    private PushConsumer consumer;
    private OMSQueue omsQueue;
    private Converter keyConverter;
    private Class<K> kClass;
    private Converter valueConverter;
    private Class<V> vClass;

    public BrokerBasedLog(MessagingAccessPoint messagingAccessPoint,
                          OMSQueue omsQueue,
                          String consumerId,
                          Callback<K, V> callback,
                          Converter keyConverter,
                          Converter valueConverter,
                          Class<K> kClass,
                          Class<V> vClass){
        this.omsQueue = omsQueue;
        this.callback = callback;
        producer = messagingAccessPoint.createProducer();
        consumer = messagingAccessPoint.createPushConsumer(
                                            OMS.newKeyValue().put(OMSBuiltinKeys.CONSUMER_ID, consumerId));
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
        this.kClass = kClass;
        this.vClass = vClass;
    }

    @Override public void start() {

        producer.startup();

        consumer.attachQueue(omsQueue.getQueue(), (message, context) -> {

            try {
                if(Long.parseLong(message.sysHeaders().getString(Message.BuiltinKeys.BORN_TIMESTAMP)) + 10000 < System.currentTimeMillis()){
                    context.ack();
                    return;
                }
                System.out.printf("Received one message: %s%n", message.sysHeaders().getString(Message.BuiltinKeys.MESSAGE_ID));
                byte[] bytes = message.getBody(byte[].class);
                Map<K, V> map = decodeKeyValue(bytes);
                for (K key : map.keySet()) {
                    callback.onCompletion(null, key, map.get(key));
                }
                context.ack();
            }catch(Exception e){
                e.printStackTrace();
            }
        });
        consumer.startup();
    }

    @Override
    public void stop(){

        producer.shutdown();
        consumer.shutdown();
    }

    @Override
    public void send(K key, V value){

        try {
            Future<SendResult> result = producer.sendAsync(producer.createBytesMessage(omsQueue.getQueue(), encodeKeyValue(key, value)));
            result.addListener((future) -> {

                if (future.getThrowable() != null) {
                    System.out.printf("Send async message Failed, error: %s%n", future.getThrowable().getMessage());
                } else {
                    System.out.printf("Send async message OK, msgId: %s%n", future.get().messageId());
                }
            });
        } catch (Exception e) {

        }
    }

    private byte[] encodeKeyValue(K key, V value) throws Exception {


        byte[] keyBtye = keyConverter.objectToByte(key);
        byte[] valueByte = valueConverter.objectToByte(value);
        Map<String, String> map = new HashMap<>();
        map.put(Base64.getEncoder().encodeToString(keyBtye), Base64.getEncoder().encodeToString(valueByte));

        return JSON.toJSONString(map).getBytes("UTF-8");
    }

    private Map<K, V> decodeKeyValue(byte[] bytes) throws Exception {

        Map<K, V> resultMap = new HashMap<>();
        String rawString = new String(bytes, "UTF-8");
        Map<String, String> map = JSON.parseObject(rawString, Map.class);
        for(String key : map.keySet()){
            K decodeKey = (K)keyConverter.byteToObject(Base64.getDecoder().decode(key));
            V decodeValue = (V)valueConverter.byteToObject(Base64.getDecoder().decode(map.get(key)));
            resultMap.put(decodeKey, decodeValue);
        }
        return resultMap;
    }

}
