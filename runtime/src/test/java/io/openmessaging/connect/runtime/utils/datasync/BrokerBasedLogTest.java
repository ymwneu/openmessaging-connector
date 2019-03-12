package io.openmessaging.connect.runtime.utils.datasync;

import io.openmessaging.*;
import io.openmessaging.connector.api.data.Converter;
import io.openmessaging.consumer.MessageListener;
import io.openmessaging.consumer.PushConsumer;
import io.openmessaging.producer.Producer;
import io.openmessaging.producer.SendResult;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class BrokerBasedLogTest {

    @Mock
    private Producer producer;

    @Mock
    private PushConsumer consumer;

    private String queueName;

    private String consumerId;

    private BrokerBasedLog brokerBasedLog;

    @Mock
    private MessagingAccessPoint messagingAccessPoint;

    @Mock
    private DataSynchronizerCallback dataSynchronizerCallback;

    @Mock
    private BytesMessage bytesMessage;

    @Mock
    private Future<SendResult> future;

    @Mock
    private Converter converter;


    @Before
    public void init() {
        queueName = "testQueueName";
        consumerId = "testConsumerId";
        doReturn(producer).when(messagingAccessPoint).createProducer();
        doReturn(consumer).when(messagingAccessPoint).createPushConsumer(any(KeyValue.class));
        doReturn(bytesMessage).when(producer).createBytesMessage(anyString(), any(byte[].class));
        doReturn(future).when(producer).sendAsync(any(Message.class));
        doReturn(new byte[0]).when(converter).objectToByte(any(Object.class));
        brokerBasedLog = new BrokerBasedLog(messagingAccessPoint, queueName, consumerId, dataSynchronizerCallback, converter, converter);
    }

    @Test
    public void testStart() {
        brokerBasedLog.start();
        verify(producer, times(1)).startup();
        verify(consumer, times(1)).attachQueue(anyString(), any(MessageListener.class));
        verify(consumer, times(1)).startup();
    }

    @Test
    public void testStop() {
        brokerBasedLog.stop();
        verify(producer, times(1)).shutdown();
        verify(consumer, times(1)).shutdown();
    }

    @Test
    public void testSend() {
        brokerBasedLog.send(new Object(), new Object());
        verify(producer, times(1)).sendAsync(any(Message.class));
    }

}