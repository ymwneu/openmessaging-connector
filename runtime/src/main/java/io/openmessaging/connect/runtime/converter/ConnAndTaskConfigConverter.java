package io.openmessaging.connect.runtime.converter;

import io.openmessaging.connect.runtime.common.ConnAndTaskConfigs;
import io.openmessaging.connect.runtime.common.LoggerName;
import io.openmessaging.connect.runtime.utils.TransferUtils;
import io.openmessaging.connector.api.data.Converter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnAndTaskConfigConverter implements Converter<ConnAndTaskConfigs> {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.OMS_RUNTIME);

    @Override
    public byte[] objectToByte(ConnAndTaskConfigs object) {
        try {
            ConnAndTaskConfigs configs = object;
            Map<String, String> connectorMap = new HashMap<>();
            Map<String, String> taskMap = new HashMap<>();
            for(String key : configs.getConnectorConfigs().keySet()){
                connectorMap.put(key, TransferUtils.keyValueToString(configs.getConnectorConfigs().get(key)));
            }
            for(String key : configs.getTaskConfigs().keySet()){
                taskMap.put(key, TransferUtils.keyValueListToString(configs.getTaskConfigs().get(key)));
            }
            return TransferUtils.toJsonString(connectorMap, taskMap).getBytes("UTF-8");
        } catch (Exception e) {
            log.error("ConnAndTaskConfigConverter#objectToByte failed", e);
        }
        return new byte[0];
    }

    @Override
    public ConnAndTaskConfigs byteToObject(byte[] bytes) {

        try {
            String jsonString = new String(bytes, "UTF-8");
            ConnAndTaskConfigs configs = TransferUtils.toConnAndTaskConfigs(jsonString);
            return configs;
        } catch (UnsupportedEncodingException e) {
            log.error("ConnAndTaskConfigConverter#byteToObject failed", e);
        }
        return null;
    }
}
