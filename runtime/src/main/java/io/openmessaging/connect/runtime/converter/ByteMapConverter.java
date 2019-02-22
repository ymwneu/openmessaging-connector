package io.openmessaging.connect.runtime.converter;

import com.alibaba.fastjson.JSON;
import io.openmessaging.connect.runtime.common.LoggerName;
import io.openmessaging.connector.api.data.Converter;
import java.io.UnsupportedEncodingException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ByteMapConverter implements Converter<Map<byte[], byte[]>> {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.OMS_RUNTIME);

    @Override
    public byte[] objectToByte(Map<byte[], byte[]> map) {

        try {
            Map<String, String> resultMap = new HashMap<>();

            for(Map.Entry<byte[], byte[]> entry : map.entrySet()){
                resultMap.put(Base64.getEncoder().encodeToString(entry.getKey()), Base64.getEncoder().encodeToString(entry.getValue()));
            }
            return JSON.toJSONString(map).getBytes("UTF-8");
        } catch (Exception e) {
            log.error("ByteMapConverter#objectToByte failed", e);
        }
        return new byte[0];
    }

    @Override
    public Map<byte[], byte[]> byteToObject(byte[] bytes) {

        Map<byte[], byte[]> resultMap = new HashMap<>();
        try {
            String rawString = new String(bytes, "UTF-8");
            Map<String, String> map = JSON.parseObject(rawString, Map.class);
            for(String key : map.keySet()){
                byte[] decodeKey = Base64.getDecoder().decode(key);
                byte[] decodeValue = Base64.getDecoder().decode(map.get(key));
                resultMap.put(decodeKey, decodeValue);
            }
            return resultMap;
        } catch (UnsupportedEncodingException e) {
            log.error("ByteMapConverter#byteToObject failed", e);
        }
        return resultMap;
    }

}
