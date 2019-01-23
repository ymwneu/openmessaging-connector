package io.openmessaging.connector.api;

import java.util.List;

public class Schema {

    enum Type {
        INT8,
    }

    private Type type;
    private Boolean isOptional;
    private String name;
    private List<Field> fields;
}
