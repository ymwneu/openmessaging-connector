package io.openmessaging.connect.runtime;

import io.openmessaging.connect.runtime.common.LoggerName;
import io.openmessaging.connect.runtime.config.ConnectConfig;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectStartup {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.OMS_RUNTIME);

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

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            private volatile boolean hasShutdown = false;
            private AtomicInteger shutdownTimes = new AtomicInteger(0);

            @Override
            public void run() {
                synchronized (this) {
                    log.info("Shutdown hook was invoked, {}", this.shutdownTimes.incrementAndGet());
                    if (!this.hasShutdown) {
                        this.hasShutdown = true;
                        long beginTime = System.currentTimeMillis();
                        controller.shutdown();
                        long consumingTimeTotal = System.currentTimeMillis() - beginTime;
                        log.info("Shutdown hook over, consuming total time(ms): {}", consumingTimeTotal);
                    }
                }
            }
        }, "ShutdownHook"));
        return controller;
    }
}
