package io.openmessaging.connector.api;

public abstract class DataEntry {

    private Long timestamp;
    private EntryType entryType;
    private Schema schema;
    private Object payload;
}
