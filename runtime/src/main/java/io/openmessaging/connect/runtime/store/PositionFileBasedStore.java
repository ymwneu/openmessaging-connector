package io.openmessaging.connect.runtime.store;

import java.util.Map;

public class OffsetFileBasedStore extends AbstractFileBaseStore<Map<Map<String, ?>, Map<String, ?>>> {

    private Map<Map<String, ?>, Map<String, ?>> offsetTable;

    @Override
    public String encode() {
        return null;
    }

    @Override
    public void decode(String jsonString) {

    }

    @Override
    public String configFilePath() {
        return null;
    }

    @Override public Map<Map<String, ?>, Map<String, ?>> getData() {
        return this.offsetTable;
    }

    @Override public void setData(Map<Map<String, ?>, Map<String, ?>> data) {
        this.offsetTable = data;
    }

}
