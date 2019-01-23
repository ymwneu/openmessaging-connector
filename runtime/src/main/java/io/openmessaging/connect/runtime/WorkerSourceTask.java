package io.openmessaging.connect.runtime;

import io.openmessaging.BytesMessage;
import io.openmessaging.Future;
import io.openmessaging.FutureListener;
import io.openmessaging.KeyValue;
import io.openmessaging.connect.runtime.cloudevents.CloudEvents;
import io.openmessaging.connect.runtime.cloudevents.Extension;
import io.openmessaging.connect.runtime.utils.Converter;
import io.openmessaging.connector.api.PositionStorageReader;
import io.openmessaging.connector.api.source.SourceDataEntry;
import io.openmessaging.connector.api.source.SourceTask;
import io.openmessaging.producer.Producer;
import io.openmessaging.producer.SendResult;
import java.net.URI;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

public class WorkerSourceTask implements Runnable {

    private SourceTask sourceTask;
    private KeyValue taskConfig;
    private AtomicBoolean isStopping;
    private PositionStorageReader positionStorageReader;
    private Converter converter;
    private Producer producer;
    private Map<Map<String, ?>, Map<String, ?>> positionData = new HashMap<>();

    public WorkerSourceTask(SourceTask sourceTask,
                            KeyValue taskConfig,
                            PositionStorageReader positionStorageReader,
                            Producer producer,
                            Converter converter){
        this.sourceTask = sourceTask;
        this.taskConfig = taskConfig;
        this.positionStorageReader = positionStorageReader;
        this.isStopping = new AtomicBoolean(false);
        this.producer = producer;
        this.converter = converter;
    }

    @Override
    public void run() {
        sourceTask.start(taskConfig);

        while(!isStopping.get()){
            Collection<SourceDataEntry> toSendEntries =  sourceTask.poll();
            if(null != toSendEntries && toSendEntries.size() > 0){
                sendRecord(toSendEntries);
            }
        }
    }

    public Map<Map<String, ?>, Map<String, ?>> getPositionData() {
        return positionData;
    }

    public void stop(){
        producer.shutdown();
    }

    private void sendRecord(Collection<SourceDataEntry> entries) {

        List<CloudEvents<SourceDataEntry>> events = parseDataEntriesToCloudEvents(entries);
        for(CloudEvents<SourceDataEntry> event : events){
            BytesMessage bytesMessage = producer.createBytesMessage("", converter.objectToByte(event));
            Future<SendResult> sendResult = producer.sendAsync(bytesMessage);
            sendResult.addListener(new FutureListener() {
                @Override public void operationComplete(Future future) {
                    // send ok
                    Map<String, ?> sourcePartition = event.getData().get().getSourcePartition();
                    Map<String, ?> sourceOffset = event.getData().get().getSourceOffset();
                    positionData.put(sourcePartition, sourceOffset);
                }
            });
        }
    }

    private List<CloudEvents<SourceDataEntry>> parseDataEntriesToCloudEvents(Collection<SourceDataEntry> entries) {

        List<CloudEvents<SourceDataEntry>> results = new ArrayList<>(entries.size());
        for(SourceDataEntry dataEntry : entries){
            CloudEvents<SourceDataEntry> cloudEvents = new CloudEvents<SourceDataEntry>() {
                @Override public String getType() {
                    return null;
                }

                @Override public String getSpecVersion() {
                    return null;
                }

                @Override public URI getSource() {
                    return null;
                }

                @Override public String getId() {
                    return null;
                }

                @Override public Optional<ZonedDateTime> getTime() {
                    return Optional.empty();
                }

                @Override public Optional<URI> getSchemaURL() {
                    return Optional.empty();
                }

                @Override public Optional<String> getContentType() {
                    return Optional.empty();
                }

                @Override public Optional<SourceDataEntry> getData() {
                    return Optional.of(dataEntry);
                }

                @Override public Optional<List<Extension>> getExtensions() {
                    return Optional.empty();
                }
            };
            results.add(cloudEvents);
        }
        return results;
    }
}
