package io.openmessaging.connect.runtime.utils;

import com.alibaba.fastjson.JSON;
import java.io.UnsupportedEncodingException;

public class BasicConverter implements Converter {

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
            Object res = JSON.parseObject(text, Object.class);
            return res;
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }
}
