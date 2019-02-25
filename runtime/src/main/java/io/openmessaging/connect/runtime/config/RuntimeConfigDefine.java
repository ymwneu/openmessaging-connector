package io.openmessaging.connect.runtime.config;

import java.util.HashSet;
import java.util.Set;

public class RuntimeConfigDefine {

    public static final String CONNECTOR_CLASS = "connector-class";

    public static final String OMS_DRIVER_URL = "oms-driver-url";

    public static final String UPDATE_TIMESATMP = "update-timestamp";

    public static final String CONFIG_DELETED = "config-deleted";

    public static final String SOURCE_RECORD_CONVERTER = "source-record-converter";

    public static final Set<String> REQUEST_CONFIG = new HashSet<String>(){
        {
            add(CONNECTOR_CLASS);
            add(OMS_DRIVER_URL);
            add(SOURCE_RECORD_CONVERTER);
        }
    };
}
