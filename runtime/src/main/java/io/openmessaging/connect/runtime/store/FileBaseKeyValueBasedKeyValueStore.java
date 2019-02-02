package io.openmessaging.connect.runtime.store;

import com.alibaba.fastjson.JSON;
import io.openmessaging.connect.runtime.common.LoggerName;
import java.io.IOException;
import java.util.Map;
import org.apache.rocketmq.common.MixAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileBaseKeyValueBasedKeyValueStore<K, V> extends MemoryBasedKeyValueStore<K, V> {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.OMS_RUNTIME);

    private String configFilePath;

    public FileBaseKeyValueBasedKeyValueStore(String configFilePath){

        super();
        this.configFilePath = configFilePath;
    }

    public String encode(){

        return JSON.toJSONString(data);
    }

    public void decode(String jsonString){

        this.data = JSON.parseObject(jsonString, Map.class);
    }

    @Override
    public boolean load() {
        String fileName = null;
        try {
            fileName = this.configFilePath;
            String jsonString = MixAll.file2String(fileName);

            if (null == jsonString || jsonString.length() == 0) {
                return this.loadBak();
            } else {
                this.decode(jsonString);
                log.info("load " + fileName + " OK");
                return true;
            }
        } catch (Exception e) {
            log.error("load " + fileName + " failed, and try to load backup file", e);
            return this.loadBak();
        }
    }

    private boolean loadBak() {
        String fileName = null;
        try {
            fileName = this.configFilePath;
            String jsonString = MixAll.file2String(fileName + ".bak");
            if (jsonString != null && jsonString.length() > 0) {
                this.decode(jsonString);
                log.info("load " + fileName + " OK");
                return true;
            }
        } catch (Exception e) {
            log.error("load " + fileName + " Failed", e);
            return false;
        }

        return true;
    }

    @Override
    public void persist() {

        String jsonString = this.encode();
        if (jsonString != null) {
            String fileName = this.configFilePath;
            try {
                MixAll.string2File(jsonString, fileName);
            } catch (IOException e) {
                log.error("persist file " + fileName + " exception", e);
            }
        }
    }
}
