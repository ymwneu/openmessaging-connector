package io.openmessaging.connector.api;

public class Field {

    private String name;
    private int index;
    private Schema schema;

    public Field(String name, int index, Schema schema) {
        this.name = name;
        this.index = index;
        this.schema = schema;
    }
}
