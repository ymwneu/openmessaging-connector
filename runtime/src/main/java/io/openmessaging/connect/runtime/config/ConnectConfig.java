package io.openmessaging.connect.runtime.config;

import java.io.File;

public class ConnectConfig {

    private String workerId = "DEFAULT_WORKER_1";
    private String storePathRootDir = System.getProperty("user.home") + File.separator + "connectorStore1";
    private String omsDriverUrl = "oms:rocketmq://localhost:9876/default:default";
    private int httpPort = 8081;

    public String getOmsDriverUrl() {
        return omsDriverUrl;
    }

    public void setOmsDriverUrl(String omsDriverUrl) {
        this.omsDriverUrl = omsDriverUrl;
    }

    public String getWorkerId() {
        return workerId;
    }

    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }

    public String getStorePathRootDir() {
        return storePathRootDir;
    }

    public void setStorePathRootDir(String storePathRootDir) {
        this.storePathRootDir = storePathRootDir;
    }

    public int getHttpPort() {
        return httpPort;
    }

    public void setHttpPort(int httpPort) {
        this.httpPort = httpPort;
    }
}
