package io.openmessaging.connect.runtime.config;

public class ConnectConfig {

    private String workerName = "DEFAULT_WORKER";
    private String omsDriverUrl = "";

    public String getOmsDriverUrl() {
        return omsDriverUrl;
    }

    public void setOmsDriverUrl(String omsDriverUrl) {
        this.omsDriverUrl = omsDriverUrl;
    }

    public String getWorkerName() {
        return workerName;
    }

    public void setWorkerName(String workerName) {
        this.workerName = workerName;
    }
}
