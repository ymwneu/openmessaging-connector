package io.openmessaging.connect.runtime.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.openmessaging.connect.runtime.ConnAndTaskConfigs;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;

public class JsonConverter implements Converter {

    @Override
    public byte[] objectToByte(Object object) {
        String json = JSON.toJSONString(object);
        try {
            return json.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("");
        }
    }

    @Override
    public Object byteToObject(byte[] bytes) {
        try {
            String text = new String(bytes, "UTF-8");
            Object res = JSON.parse(text);

            return res;
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }
}
