package io.openmessaging.connect.runtime.utils;

import java.io.File;

public class FilePathConfigUtil {

    public static String getConnectorConfigPath(final String rootDir) {
        return rootDir + File.separator + "config" + File.separator + "connectorConfig.json";
    }

    public static String getTaskConfigPath(final String rootDir) {
        return rootDir + File.separator + "config" + File.separator + "taskConfig.json";
    }

    public static String getPositionPath(final String rootDir) {
        return rootDir + File.separator + "config" + File.separator + "position.json";
    }}
