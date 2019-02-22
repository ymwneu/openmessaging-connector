package io.openmessaging.connect.runtime.converter;

import com.alibaba.fastjson.JSON;
import io.openmessaging.connect.runtime.common.LoggerName;
import io.openmessaging.connector.api.data.Converter;
import java.io.UnsupportedEncodingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonConverter implements Converter {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.OMS_RUNTIME);

    private Class clazz;

    public JsonConverter(){
        this.clazz = null;
    }

    public JsonConverter(Class clazz){
        this.clazz = clazz;
    }

    @Override
    public byte[] objectToByte(Object object) {
        try {
            String json = JSON.toJSONString(object);
            return json.getBytes("UTF-8");
        } catch (Exception e) {
            log.error("JsonConverter#objectToByte failed", e);
        }
        return new byte[0];
    }

    @Override
    public Object byteToObject(byte[] bytes) {
        try {
            String text = new String(bytes, "UTF-8");

            Object res;
            if(clazz != null){
                res = JSON.parseObject(text, clazz);
            }else {
                res = JSON.parse(text);
            }
            return res;
        } catch (UnsupportedEncodingException e) {
            log.error("JsonConverter#byteToObject failed", e);
        }
        return null;
    }
}
