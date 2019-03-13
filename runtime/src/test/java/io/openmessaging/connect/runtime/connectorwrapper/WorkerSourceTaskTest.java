package io.openmessaging.connect.runtime.connectorwrapper;

import io.openmessaging.BytesMessage;
import io.openmessaging.Message;
import io.openmessaging.MessagingAccessPoint;
import io.openmessaging.connect.runtime.common.ConnectKeyValue;
import io.openmessaging.connect.runtime.connectorwrapper.testimpl.TestConverter;
import io.openmessaging.connect.runtime.connectorwrapper.testimpl.TestPositionStorageReader;
import io.openmessaging.connect.runtime.connectorwrapper.testimpl.TestSourceTask;
import io.openmessaging.producer.Producer;
import io.openmessaging.rocketmq.domain.BytesMessageImpl;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class WorkerSourceTaskTest {

    @Mock
    private Producer producer;

    @Mock
    private MessagingAccessPoint messagingAccessPoint;

    private WorkerSourceTask workerSourceTask;

    @Test
    public void testRun() {
        doReturn(producer).when(messagingAccessPoint).createProducer();
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                final String queue = invocationOnMock.getArgument(0);
                final byte[] body = invocationOnMock.getArgument(1);
                BytesMessage message = new BytesMessageImpl();
                message.setBody(body);
                message.sysHeaders().put("DESTINATION", queue);
                return message;
            }
        }).when(producer).createBytesMessage(anyString(), any(byte[].class));
        ConnectKeyValue connectKeyValue = new ConnectKeyValue();
        connectKeyValue.getProperties().put("key1", "TEST-TASK-1");
        workerSourceTask = new WorkerSourceTask("TEST-CONN",
                new TestSourceTask(),
                connectKeyValue,
                new TestPositionStorageReader(),
                new TestConverter(),
                producer
        );
        workerSourceTask.run();
        producer.startup();
        verify(producer, times(1)).startup();
        verify(producer, times(1)).sendAsync(any(Message.class));
    }
}
