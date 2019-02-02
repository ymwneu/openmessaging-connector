package io.openmessaging.connect.runtime.utils;

import io.openmessaging.KeyValue;
import io.openmessaging.connect.runtime.ConnAndTaskConfigs;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConnAndTaskConfigConverter implements Converter {

    @Override public byte[] objectToByte(Object object) {
        ConnAndTaskConfigs configs = (ConnAndTaskConfigs) object;
        Map<String, String> connectorMap = new HashMap<>();
        Map<String, String> taskMap = new HashMap<>();
        for(String key : configs.getConnectorConfigs().keySet()){
            connectorMap.put(key, TransferUtils.keyValueToString(configs.getConnectorConfigs().get(key)));
        }
        for(String key : configs.getTaskConfigs().keySet()){
            taskMap.put(key, TransferUtils.keyValueListToString(configs.getTaskConfigs().get(key)));
        }
        try {
            return TransferUtils.toJsonString(connectorMap, taskMap).getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
        }
        return null;
    }

    @Override public Object byteToObject(byte[] bytes) {

        try {
            String jsonString = new String(bytes, "UTF-8");
            ConnAndTaskConfigs configs = TransferUtils.toConnAndTaskConfigs(jsonString);
            return configs;
        } catch (UnsupportedEncodingException e) {
        }
        return null;
    }
}
