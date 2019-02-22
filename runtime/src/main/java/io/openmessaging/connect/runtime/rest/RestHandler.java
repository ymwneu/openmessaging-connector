package io.openmessaging.connect.runtime.rest;

import com.alibaba.fastjson.JSON;
import io.javalin.Context;
import io.javalin.Javalin;
import io.openmessaging.connect.runtime.ConnectController;
import io.openmessaging.connect.runtime.common.LoggerName;
import io.openmessaging.connect.runtime.common.ConnectKeyValue;
import io.openmessaging.connect.runtime.connectorwrapper.WorkerConnector;
import io.openmessaging.connect.runtime.connectorwrapper.WorkerSourceTask;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
        app.get("/connectors/:connectorName/stop", this::handleStopConnector);
        app.get("/getClusterInfo", this::getClusterInfo);
        app.get("/getConfigInfo", this::getConfigInfo);
        app.get("/getPositionInfo", this::getPositionInfo);
        app.get("/getAllocatedInfo", this::getAllocatedInfo);
    }

    private void getAllocatedInfo(Context context){

        Set<WorkerConnector> workerConnectors = connectController.getWorker().getWorkingConnectors();
        Set<WorkerSourceTask> workerSourceTasks = connectController.getWorker().getWorkingTasks();
        StringBuilder sb = new StringBuilder();
        sb.append("working connectors:\n");
        for(WorkerConnector workerConnector : workerConnectors){
            sb.append(workerConnector.toString()+"\n");
        }
        sb.append("working tasks:\n");
        for(WorkerSourceTask workerSourceTask : workerSourceTasks){
            sb.append(workerSourceTask.toString()+"\n");
        }
        context.result(sb.toString());
    }

    private void getPositionInfo(Context context) {

        Map<byte[], byte[]> positionTable = connectController.getPositionManagementService().getPositionTable();
        context.result("positionTable:"+JSON.toJSONString(positionTable));
    }

    private void getConfigInfo(Context context) {

        Map<String, ConnectKeyValue> connectorConfigs = connectController.getConfigManagementService().getConnectorConfigs();
        Map<String, List<ConnectKeyValue>> taskConfigs = connectController.getConfigManagementService().getTaskConfigs();
        context.result("ConnectorConfigs:"+JSON.toJSONString(connectorConfigs)+"\nTaskConfigs:"+JSON.toJSONString(taskConfigs));
    }

    private void getClusterInfo(Context context) {
        context.result(JSON.toJSONString(connectController.getClusterManagementService().getAllAliveWorkers()));
    }

    private void handleCreateConnector(Context context) {
        String connectorName = context.param("connectorName");
        String arg = context.queryParam("config");
        Map keyValue = JSON.parseObject(arg, Map.class);
        ConnectKeyValue configs = new ConnectKeyValue();
        for(Object key : keyValue.keySet()){
            configs.put((String)key, (String)keyValue.get(key));
        }
        try {

            String result = connectController.getConfigManagementService().putConnectorConfig(connectorName, configs);
            if(result != null && result.length() > 0){
                context.result(result);
            }else{
                context.result("success");
            }
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
        String connectorName = context.param("connectorName");
        try {

            connectController.getConfigManagementService().removeConnectorConfig(connectorName);
            context.result("success");
        } catch (Exception e) {
            context.result("failed");
        }
    }
}
