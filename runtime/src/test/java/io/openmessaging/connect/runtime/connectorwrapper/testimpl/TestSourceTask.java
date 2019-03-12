package io.openmessaging.connect.runtime.connectorwrapper.testimpl;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.EntryType;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SourceDataEntry;
import io.openmessaging.connector.api.source.SourceTask;

import java.util.*;

public class TestSourceTask extends SourceTask {

    @Override
    public Collection<SourceDataEntry> poll() {
        Set<SourceDataEntry> sourceTasks = new HashSet<>();
        Object[] newPayload = new Object[1];
        newPayload[0] = Base64.getEncoder().encodeToString("test".getBytes());
        sourceTasks.add(new SourceDataEntry(
                "1".getBytes(),
                "2".getBytes(),
                System.currentTimeMillis(),
                EntryType.CREATE,
                "test-queue",
                new Schema(),
                newPayload
        ));
        return sourceTasks;
    }

    @Override
    public void start(KeyValue config) {

    }

    @Override
    public void stop() {

    }

    @Override
    public void pause() {

    }

    @Override
    public void resume() {

    }
}
