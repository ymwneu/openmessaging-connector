package io.openmessaging.connect.runtime.rest;

import com.alibaba.fastjson.JSON;
import io.javalin.Context;
import io.javalin.Javalin;
import io.openmessaging.KeyValue;
import io.openmessaging.connect.runtime.ConnectController;
import io.openmessaging.connect.runtime.common.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestHandler {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.OMS_RUNTIME);

    private final ConnectController connectController;

    public RestHandler(ConnectController connectController){
        this.connectController = connectController;
        Javalin app = Javalin.start(8080);
        app.post("/connectors/:connectorName", this::handleCreateConnector);
        app.get("/connectors/:connectorName/config", this::handleQueryConnectorConfig);
        app.get("/connectors/:connectorName/status", this::handleQueryConnectorStatus);
        app.post("/connectors/:connectorName/stop", this::handleStopConnector);
    }

    private void handleCreateConnector(Context context) {
        String body = context.body();
        String connectorName = context.param("connectorName");
        KeyValue keyValue = JSON.parseObject(body, KeyValue.class);
    }

    private String handleQueryConnectorConfig(Context context){
        context.result("ok");
        return "ok";
    }

    private void handleQueryConnectorStatus(Context context){

    }

    private void handleStopConnector(Context context){

    }
}
