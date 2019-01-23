package io.openmessaging.connect.runtime.store;

import io.openmessaging.connect.runtime.ConnAndTaskConfigs;

public class ConnectorConfigFileBaseStore extends AbstractFileBaseStore<ConnAndTaskConfigs>{

    private ConnAndTaskConfigs configs;

    @Override public String encode() {
        return null;
    }

    @Override public void decode(String jsonString) {

    }

    @Override public String configFilePath() {
        return null;
    }

    @Override public ConnAndTaskConfigs getData() {
        return null;
    }

    @Override public void setData(ConnAndTaskConfigs data) {
        configs = data;
    }

}
