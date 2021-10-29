package client.miscellaneous;

import client.supplements.ExceptionHandler;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadTest {
    private static final Logger LOG = Logger.getLogger(ThreadTest.class);
    private static final AtomicInteger COUNT = new AtomicInteger(0);

    public static void main(final String[] args) {

        Path currentRelativePath = Paths.get("");
        String currentAbsolutePath = currentRelativePath.toAbsolutePath().toString();
        String filePath = "/configs/";
        String logPropertiesName = "log4j.properties";

        PropertyConfigurator.configureAndWatch(currentAbsolutePath + filePath + logPropertiesName, 60 * 1000);

        Thread.setDefaultUncaughtExceptionHandler(
                (t, ex) -> {
                    LOG.error(t.getName() + ": " + ex.getMessage() + " exception with id #" + COUNT.get());
                    LOG.info(COUNT.get() + " final count");
                    System.exit(0);
                });

        while (true) {
            new Thread(() -> {
                synchronized (COUNT) {
                    COUNT.getAndIncrement();
                    LOG.info("New thread #" + COUNT.get());
                }
                while (true) {
                    try {
                        Thread.sleep(1000);
                    } catch (Exception ex) {
                        ExceptionHandler.logException(ex);
                        System.exit(0);
                    }
                }
            }).start();

        }
    }
}
