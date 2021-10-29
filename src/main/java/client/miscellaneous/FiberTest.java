package client.miscellaneous;

import client.supplements.ExceptionHandler;
import co.paralleluniverse.fibers.Fiber;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

public class FiberTest {
    private static final Logger LOG = Logger.getLogger(FiberTest.class);
    private static final AtomicInteger COUNT = new AtomicInteger(0);

    public static void main(final String[] args) {


        Path currentRelativePath = Paths.get("");
        String currentAbsolutePath = currentRelativePath.toAbsolutePath().toString();
        String filePath = "/configs/";
        String logPropertiesName = "log4j.properties";

        PropertyConfigurator.configureAndWatch(currentAbsolutePath + filePath + logPropertiesName, 60 * 1000);

        Fiber.setDefaultUncaughtExceptionHandler(
                (t, ex) -> {
                    LOG.error(t.getName() + ": " + ex.getMessage() + " exception with id #" + COUNT.get());
                    LOG.info(COUNT.get() + " final count");
                    System.exit(0);
                });

        while (true) {
            new Fiber<Void>() {
                @Override
                protected Void run() {
                    synchronized (COUNT) {
                        COUNT.getAndIncrement();
                        LOG.info("New fiber #" + COUNT.get());
                    }
                    while (true) {
                        try {
                            Fiber.sleep(1000);
                        } catch (Throwable ex) {
                            ExceptionHandler.logException(ex);
                            System.exit(0);
                        }
                    }
                }
            }.start();

        }
    }
}
