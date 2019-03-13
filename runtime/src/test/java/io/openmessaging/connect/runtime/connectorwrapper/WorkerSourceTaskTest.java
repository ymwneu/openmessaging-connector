/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

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

    private WorkerSourceTask workerSourceTask;

    @Test
    public void testRun() {

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
        connectKeyValue.put("key1", "TEST-TASK-1");
        workerSourceTask = new WorkerSourceTask("TEST-CONN",
                new TestSourceTask(),
                connectKeyValue,
                new TestPositionStorageReader(),
                new TestConverter(),
                producer
        );
        workerSourceTask.run();
        verify(producer, times(1)).sendAsync(any(Message.class));
    }
}
