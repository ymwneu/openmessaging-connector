package io.openmessaging.connect.runtime;

import io.openmessaging.connect.runtime.common.LoggerName;
import io.openmessaging.connect.runtime.config.ConnectConfig;
import io.openmessaging.connect.runtime.utils.FileAndPropertyUtil;
import io.openmessaging.connect.runtime.utils.ServerUtil;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectStartup {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.OMS_RUNTIME);

    public static CommandLine commandLine = null;

    public static String configFile = null;

    public static Properties properties = null;

    public static void main(String[] args) {
        start(createConnectController(args));
    }

    private static void start(ConnectController controller) {
        controller.start();
    }

    private static ConnectController createConnectController(String[] args) {

        try {

            Options options = ServerUtil.buildCommandlineOptions(new Options());
            commandLine = ServerUtil.parseCmdLine("connect", args, buildCommandlineOptions(options),
                new PosixParser());
            if (null == commandLine) {
                System.exit(-1);
            }

            ConnectConfig connectConfig = new ConnectConfig();
            if (commandLine.hasOption('c')) {
                String file = commandLine.getOptionValue('c');
                if (file != null) {
                    configFile = file;
                    InputStream in = new BufferedInputStream(new FileInputStream(file));
                    properties = new Properties();
                    properties.load(in);

                    FileAndPropertyUtil.properties2Object(properties, connectConfig);

                    in.close();
                }
            }

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

        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return null;
    }

    private static Options buildCommandlineOptions(Options options) {

        Option opt = new Option("c", "configFile", true, "connect config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }
}
