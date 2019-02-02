package io.openmessaging.connect.runtime.rest;

import com.alibaba.fastjson.JSON;
import io.javalin.Context;
import io.javalin.Javalin;
import io.openmessaging.KeyValue;
import io.openmessaging.connect.runtime.ConnectController;
import io.openmessaging.connect.runtime.common.LoggerName;
import io.openmessaging.connect.runtime.utils.ConnectKeyValue;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestHandler {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.OMS_RUNTIME);

    private final ConnectController connectController;

    public RestHandler(ConnectController connectController){
        this.connectController = connectController;
        Javalin app = Javalin.start(connectController.getConnectConfig().getHttpPort());
        app.get("/connectors/:connectorName", this::handleCreateConnector);
        app.get("/connectors/:connectorName/config", this::handleQueryConnectorConfig);
        app.get("/connectors/:connectorName/status", this::handleQueryConnectorStatus);
        app.post("/connectors/:connectorName/stop", this::handleStopConnector);
        app.get("/get", this::getInfo);
    }

    private void getInfo(Context context) {
        context.result(JSON.toJSONString(connectController.getClusterManagementService().getAllAliveWorkers()));
    }

    private void handleCreateConnector(Context context) {
        String connectorName = context.param("connectorName");
        String arg = context.queryParam("config");
        Map keyValue = JSON.parseObject(arg, Map.class);
        KeyValue configs = new ConnectKeyValue();
        for(Object key : keyValue.keySet()){
            configs.put((String)key, (String)keyValue.get(key));
        }
        try {

            connectController.getConfigManagementService().putConnectorConfig(connectorName, configs);
            context.result("success");
        } catch (Exception e) {
            context.result("failed");
        }
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
