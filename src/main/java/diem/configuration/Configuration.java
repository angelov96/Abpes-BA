package diem.configuration;

import client.supplements.ExceptionHandler;
import diem.payloads.IDiemPayload;
import diem.payloads.IDiemReadPayload;
import diem.payloads.IDiemWritePayload;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.nio.file.Path;
import java.nio.file.Paths;

public final class Configuration {
    private static final Path CURRENT_RELATIVE_PATH = Paths.get("");
    private static final String CURRENT_ABSOLUTE_PATH = CURRENT_RELATIVE_PATH.toAbsolutePath().toString();
    public static final String FILE_NAME = "diemConfiguration.properties";
    public static final String FILE_PATH = "/configs/";

    private static PropertiesConfiguration PROPERTIES_CONFIGURATION;


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

    public static final String JSON_RPC_URL = PROPERTIES_CONFIGURATION.getString("JSON_RPC_URL");
    public static final int CHAIN_ID = PROPERTIES_CONFIGURATION.getInt("CHAIN_ID");

    public static final boolean PREPARE_READ_PAYLOADS = PROPERTIES_CONFIGURATION.getBoolean("PREPARE_READ_PAYLOADS");
    public static final boolean PREPARE_WRITE_PAYLOADS = PROPERTIES_CONFIGURATION.getBoolean("PREPARE_WRITE_PAYLOADS");

    public static final boolean ENABLE_LISTENER= PROPERTIES_CONFIGURATION.getBoolean("ENABLE_LISTENER");
    public static final int NUMBER_OF_LISTENERS= PROPERTIES_CONFIGURATION.getInt("NUMBER_OF_LISTENERS");
    public static final double LISTENER_THRESHOLD = PROPERTIES_CONFIGURATION.getDouble("LISTENER_THRESHOLD");
    public static final double LISTENER_TOTAL_THRESHOLD = PROPERTIES_CONFIGURATION.getDouble(
            "LISTENER_TOTAL_THRESHOLD");
    public static final long TIMEOUT_LISTENER = PROPERTIES_CONFIGURATION.getLong("TIMEOUT_LISTENER");

    public static final int NUMBER_OF_TRANSACTIONS_PER_CLIENT= PROPERTIES_CONFIGURATION.getInt("NUMBER_OF_TRANSACTIONS_PER_CLIENT");

    public static final int KEY_VALUE_STRING_LENGTH = PROPERTIES_CONFIGURATION.getInt("KEY_VALUE_STRING_LENGTH");

    public static final int CHECKING_BALANCE = PROPERTIES_CONFIGURATION.getInt("CHECKING_BALANCE");
    public static final int SAVINGS_BALANCE = PROPERTIES_CONFIGURATION.getInt("SAVINGS_BALANCE");
    public static final int WRITE_CHECK_AMOUNT = PROPERTIES_CONFIGURATION.getInt("WRITE_CHECK_AMOUNT");
    public static final int DEPOSIT_CHECK_AMOUNT = PROPERTIES_CONFIGURATION.getInt("DEPOSIT_CHECK_AMOUNT");
    public static final int TRANSACT_SAVINGS_AMOUNT = PROPERTIES_CONFIGURATION.getInt("TRANSACT_SAVINGS_AMOUNT");
    public static final int SEND_PAYMENT_AMOUNT = PROPERTIES_CONFIGURATION.getInt("SEND_PAYMENT_AMOUNT");
    public static final int SEND_CYCLE = PROPERTIES_CONFIGURATION.getInt("SEND_CYCLE");

    public static Class<? extends IDiemWritePayload> WRITE_PAYLOAD;
    public static Class<? extends IDiemReadPayload> READ_PAYLOAD;

    public static Class<? extends IDiemPayload> WRITE_PAYLOAD_PATTERN;

    public static Class<? extends IDiemPayload> READ_PAYLOAD_PATTERN;

    static {
        try {
            WRITE_PAYLOAD_PATTERN = (Class<? extends IDiemPayload>) Class.forName(PROPERTIES_CONFIGURATION.getString("WRITE_PAYLOAD_PATTERN"));
        } catch (ClassNotFoundException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    static {
        try {
            READ_PAYLOAD_PATTERN = (Class<? extends IDiemPayload>) Class.forName(PROPERTIES_CONFIGURATION.getString("READ_PAYLOAD_PATTERN"));
        } catch (ClassNotFoundException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    static {
        try {
            WRITE_PAYLOAD = (Class<? extends IDiemWritePayload>) Class.forName(PROPERTIES_CONFIGURATION.getString(
                    "WRITE_PAYLOAD"));
        } catch (ClassNotFoundException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    static {
        try {
            READ_PAYLOAD = (Class<? extends IDiemReadPayload>) Class.forName(PROPERTIES_CONFIGURATION.getString(
                    "READ_PAYLOAD"));
        } catch (ClassNotFoundException ex) {
            ExceptionHandler.logException(ex);
        }
    }

}
