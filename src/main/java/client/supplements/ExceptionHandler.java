package client.supplements;

import client.configuration.GeneralConfiguration;
import co.paralleluniverse.fibers.Suspendable;
import org.apache.log4j.Logger;

public class ExceptionHandler {
    private static final Logger LOG = Logger.getLogger(ExceptionHandler.class);
    private static final int STATUS = 0;

    private static final int NON_EXIT_STATUS = 99;

    @Suspendable
    public static void logException(final Throwable ex) {
        logException(ex, false);
    }

    @Suspendable
    public static void logException(final Throwable ex, final boolean exit) {
        logException(ex, exit, STATUS);
    }

    @Suspendable
    public static void logException(final Throwable ex, final boolean exit, final int status) {
        if (ex instanceof Exception) {
            LOG.error("Exception: " + ex.getMessage());
        } else {
            LOG.error(ex.getMessage());
        }
        if (GeneralConfiguration.PRINT_STACK_TRACE) {
            ex.printStackTrace();
        }
        if (exit) {
            LOG.error("Exiting due to exception");
            System.exit(status);
        }
        if (GeneralConfiguration.FORCE_QUIT_ON_EXCEPTION && status != NON_EXIT_STATUS) {
            LOG.error("Forced exiting due to exception: " + ex.getMessage());
            System.exit(status);
        }
    }

}
