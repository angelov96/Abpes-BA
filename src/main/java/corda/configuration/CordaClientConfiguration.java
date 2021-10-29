package corda.configuration;

import client.supplements.ExceptionHandler;
import net.corda.client.rpc.CordaRPCClientConfiguration;
import net.corda.core.internal.CordaUtilsKt;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;

public class CordaClientConfiguration {

    private static final Path CURRENT_RELATIVE_PATH = Paths.get("");
    private static final String CURRENT_ABSOLUTE_PATH = CURRENT_RELATIVE_PATH.toAbsolutePath().toString();
    public static final String FILE_NAME = "cordaClientConfiguration.properties";
    public static final String FILE_PATH = "/configs/";

    static {
        try {
            PROPERTIES_CONFIGURATION =
                    new FileBasedConfigurationBuilder<>(PropertiesConfiguration.class)
                            .configure(new Parameters()
                                    .properties()
                                    .setBasePath(CURRENT_ABSOLUTE_PATH + FILE_PATH)
                                    .setFileName(FILE_NAME)
                                    .setListDelimiterHandler(new DefaultListDelimiterHandler(',')))
                            .getConfiguration();
        } catch (ConfigurationException ex) {
            ExceptionHandler.logException(ex);
        }

    }

    private static PropertiesConfiguration PROPERTIES_CONFIGURATION;

    public static int MINIMUM_SERVER_PROTOCOL_VERSION;

    public static final Duration CONNECTION_MAX_RETRY_INTERVAL = Duration.ofMinutes(PROPERTIES_CONFIGURATION.getInt(
            "CONNECTION_MAX_RETRY_INTERVAL"));
    public static final boolean TRACK_RPC_CALL_SITES = PROPERTIES_CONFIGURATION.getBoolean("TRACK_RPC_CALL_SITES");
    public static final Duration REAP_INTERVAL = Duration.ofMinutes(PROPERTIES_CONFIGURATION.getInt("REAP_INTERVAL"));
    public static final int OBSERVATION_EXECUTOR_POOL_SIZE = PROPERTIES_CONFIGURATION.getInt(
            "OBSERVATION_EXECUTOR_POOL_SIZE");
    public static final int CACHE_CONCURRENCY_LEVEL = PROPERTIES_CONFIGURATION.getInt("CACHE_CONCURRENCY_LEVEL");
    private static final Duration CONNECTION_RETRY_INTERVAL = Duration.ofMinutes(PROPERTIES_CONFIGURATION.getInt(
            "CONNECTION_RETRY_INTERVAL"));
    private static final double CONNECTION_RETRY_INTERVAL_MULTIPLIER = PROPERTIES_CONFIGURATION.getDouble(
            "CONNECTION_RETRY_INTERVAL_MULTIPLIER");
    private static final int MAX_RECONNECT_ATTEMPTS = PROPERTIES_CONFIGURATION.getInt("MAX_RECONNECT_ATTEMPTS");
    private static final int MAX_FILE_SIZE = PROPERTIES_CONFIGURATION.getInt("MAX_FILE_SIZE");
    private static final Duration DEDUPLICATION_CACHE_EXPIRY = Duration.ofDays(PROPERTIES_CONFIGURATION.getInt(
            "DEDUPLICATION_CACHE_EXPIRY"));

    {
        try {
            MINIMUM_SERVER_PROTOCOL_VERSION =
                    (int) CordaUtilsKt.class.getDeclaredField(PROPERTIES_CONFIGURATION.getString(
                            "MINIMUM_SERVER_PROTOCOL_VERSION")).get(null);
        } catch (IllegalAccessException | NoSuchFieldException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    public CordaRPCClientConfiguration getCustomClientConfiguration() {
        return new CordaRPCClientConfiguration(CONNECTION_MAX_RETRY_INTERVAL, MINIMUM_SERVER_PROTOCOL_VERSION,
                TRACK_RPC_CALL_SITES, REAP_INTERVAL, OBSERVATION_EXECUTOR_POOL_SIZE, CACHE_CONCURRENCY_LEVEL,
                CONNECTION_RETRY_INTERVAL, CONNECTION_RETRY_INTERVAL_MULTIPLIER, MAX_RECONNECT_ATTEMPTS,
                MAX_FILE_SIZE, DEDUPLICATION_CACHE_EXPIRY);
    }
}
