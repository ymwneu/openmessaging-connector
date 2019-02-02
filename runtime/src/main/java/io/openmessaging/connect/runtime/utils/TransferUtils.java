package io.openmessaging.connect.runtime.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.openmessaging.KeyValue;
import io.openmessaging.connect.runtime.ConnAndTaskConfigs;
import io.openmessaging.internal.DefaultKeyValue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransferUtils {

    public static String keyValueToString(KeyValue keyValue){

        Map<String, String> resMap = new HashMap<>();
        if (null == keyValue) {
            return JSON.toJSONString(resMap);
        }
        for(String key : keyValue.keySet()){
            resMap.put(key, keyValue.getString(key));
        }
        return JSON.toJSONString(resMap);
    }

    public static String keyValueListToString(List<KeyValue> keyValueList){


        List<Map<String, String>> resList = new ArrayList<>();
        if (null == keyValueList || 0 == keyValueList.size()) {
            return JSON.toJSONString(resList);
        }
        for(KeyValue keyValue : keyValueList){
            Map<String, String> resMap = new HashMap<>();
            for(String key : keyValue.keySet()){
                resMap.put(key, keyValue.getString(key));
            }
            resList.add(resMap);
        }
        return JSON.toJSONString(resList);
    }

    public static KeyValue stringToKeyValue(String json){

        if(null == json){
            return new DefaultKeyValue();
        }
        Map<String, String> map = JSON.parseObject(json, Map.class);
        KeyValue keyValue = new DefaultKeyValue();
        for(String key : map.keySet()){
            keyValue.put(key, map.get(key));
        }
        return keyValue;
    }

    public static List<KeyValue> stringToKeyValueList(String json){

        List<KeyValue> resultList = new ArrayList<>();
        if(null == json){
            return resultList;
        }
        List<Map<String, String>> list = JSON.parseObject(json, List.class);
        for(Map<String, String> map : list){
            KeyValue keyValue = new DefaultKeyValue();
            for(String key : map.keySet()){
                keyValue.put(key, map.get(key));
            }
            resultList.add(keyValue);
        }
        return resultList;
    }

    public static String toJsonString(Map<String, String> connectorConfigs, Map<String, String> taskConfigs){

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("connector", connectorConfigs);
        jsonObject.put("task", taskConfigs);
        return jsonObject.toString();
    }

    public static ConnAndTaskConfigs toConnAndTaskConfigs(String json){

        JSONObject jsonObject = JSON.parseObject(json, JSONObject.class);

        Map<String, String> connectorConfigs = (Map<String, String>)jsonObject.getObject("connector", Map.class);
        Map<String, String> taskConfigs = (Map<String, String>)jsonObject.getObject("task", Map.class);


        Map<String, KeyValue> transferedConnectorConfigs = new HashMap<>();
        for(String key : connectorConfigs.keySet()){
            transferedConnectorConfigs.put(key, stringToKeyValue(connectorConfigs.get(key)));
        }
        Map<String, List<KeyValue>> transferedTasksConfigs = new HashMap<>();
        for(String key : taskConfigs.keySet()){
            transferedTasksConfigs.put(key, stringToKeyValueList(taskConfigs.get(key)));
        }

        ConnAndTaskConfigs res = new ConnAndTaskConfigs();
        res.setConnectorConfigs(transferedConnectorConfigs);
        res.setTaskConfigs(transferedTasksConfigs);
        return res;
    }

}
