package io.openmessaging.connect.runtime.converter;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import io.openmessaging.connect.runtime.common.LoggerName;
import io.openmessaging.connector.api.data.Converter;
import java.io.UnsupportedEncodingException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ListConverter implements Converter<List> {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.OMS_RUNTIME);

    private Class clazz;

    public ListConverter(Class clazz){
        this.clazz = clazz;
    }

    @Override
    public byte[] objectToByte(List list) {
        try {
            return JSON.toJSONString(list).getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            log.error("ListConverter#objectToByte failed", e);
        }
        return null;
    }

    @Override
    public List byteToObject(byte[] bytes) {
        try {
            String json = new String(bytes, "UTF-8");
            List list = JSONArray.parseArray(json, clazz);
            return list;
        } catch (UnsupportedEncodingException e) {
            log.error("ListConverter#byteToObject failed", e);
        }
        return null;
    }
}
