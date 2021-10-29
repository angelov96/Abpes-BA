package client.configuration;

import client.supplements.ExceptionHandler;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.nio.file.Path;
import java.nio.file.Paths;

public class DistributionConfiguration {

    private static final Path CURRENT_RELATIVE_PATH = Paths.get("");
    private static final String CURRENT_ABSOLUTE_PATH = CURRENT_RELATIVE_PATH.toAbsolutePath().toString();
    public static final String FILE_NAME = "distributionConfiguration.properties";
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

    public static final boolean BEFORE_WAIT_FOR_CLIENTS = PROPERTIES_CONFIGURATION.getBoolean(
            "BEFORE_WAIT_FOR_CLIENTS");
    public static final boolean AFTER_WAIT_FOR_CLIENTS = PROPERTIES_CONFIGURATION.getBoolean("AFTER_WAIT_FOR_CLIENTS");
    public static final boolean AFTER_WAIT_FOR_CLIENTS_AFTER_MEASUREMENT_POINT = PROPERTIES_CONFIGURATION.getBoolean(
            "AFTER_WAIT_FOR_CLIENTS_AFTER_MEASUREMENT_POINT");
    public static final boolean BEFORE_WORKLOAD_RATE_LIMITER = PROPERTIES_CONFIGURATION.getBoolean(
            "BEFORE_WORKLOAD_RATE_LIMITER");
    public static final boolean AFTER_WORKLOAD_RATE_LIMITER = PROPERTIES_CONFIGURATION.getBoolean(
            "AFTER_WORKLOAD_RATE_LIMITER");
    public static final boolean BEFORE_WORKLOAD_MEASUREMENT = PROPERTIES_CONFIGURATION.getBoolean(
            "BEFORE_WORKLOAD_MEASUREMENT");
    public static final boolean AFTER_WORKLOAD_MEASUREMENT = PROPERTIES_CONFIGURATION.getBoolean(
            "AFTER_WORKLOAD_MEASUREMENT");

}
