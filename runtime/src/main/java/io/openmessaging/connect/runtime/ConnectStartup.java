package io.openmessaging.connect.runtime;

import io.openmessaging.MessagingAccessPoint;
import io.openmessaging.connect.runtime.config.ConnectConfig;

public class ConnectStartup {

    public static void main(String[] args) {
        start(createConnectController(args));
    }

    private static void start(ConnectController controller) {
        controller.start();
    }

    private static ConnectController createConnectController(String[] args) {

        ConnectConfig connectConfig = new ConnectConfig();
        ConnectController controller = new ConnectController(connectConfig);
        controller.initialize();
        return controller;
    }
}
