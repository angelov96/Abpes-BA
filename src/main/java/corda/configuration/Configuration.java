package corda.configuration;

import client.configuration.GeneralConfiguration;
import client.supplements.ExceptionHandler;
import corda.listener.CloseFlowPoint;
import corda.payload_patterns.ICordaPayloads;
import corda.payloads.GeneralReadPayload;
import corda.payloads.ICordaReadPayload;
import corda.payloads.ICordaWritePayload;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;

public final class Configuration {

    private static final Path CURRENT_RELATIVE_PATH = Paths.get("");
    private static final String CURRENT_ABSOLUTE_PATH = CURRENT_RELATIVE_PATH.toAbsolutePath().toString();
    public static final String FILE_NAME = "cordaConfiguration.properties";
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

    public static Class<? extends ICordaWritePayload> WRITE_PAYLOAD;
    public static Class<? extends ICordaReadPayload> READ_PAYLOAD = GeneralReadPayload.class;
    public static final boolean USE_CUSTOM_CLIENT_CONFIGURATION = PROPERTIES_CONFIGURATION.getBoolean(
            "USE_CUSTOM_CLIENT_CONFIGURATION");
    public static final int MAX_GRACEFUL_RECONNECTS = PROPERTIES_CONFIGURATION.getInt("MAX_GRACEFUL_RECONNECTS");
    public static final String RPC_USER = PROPERTIES_CONFIGURATION.getString("RPC_USER");
    public static final String RPC_PASSWORD = PROPERTIES_CONFIGURATION.getString("RPC_PASSWORD");
    public static final int NUMBER_OF_TRANSACTIONS_PER_CLIENT = PROPERTIES_CONFIGURATION.getInt(
            "NUMBER_OF_TRANSACTIONS_PER_CLIENT");
    public static final int RESEND_TIMES_UPON_ERROR_WRITE = PROPERTIES_CONFIGURATION.getInt(
            "RESEND_TIMES_UPON_ERROR_WRITE");
    public static final int RESEND_TIMES_UPON_ERROR_READ = PROPERTIES_CONFIGURATION.getInt(
            "RESEND_TIMES_UPON_ERROR_READ");

    public static final long TIMEOUT_TRANSACTION = PROPERTIES_CONFIGURATION.getInt("TIMEOUT_TRANSACTION");
    public static final TimeUnit TIMEOUT_UNIT_TRANSACTION = PROPERTIES_CONFIGURATION.get(TimeUnit.class, "TIMEOUT_UNIT_TRANSACTION");
    public static final boolean DROP_ON_TIMEOUT = PROPERTIES_CONFIGURATION.getBoolean("DROP_ON_TIMEOUT");

    public static Class<? extends ICordaPayloads> WRITE_PAYLOAD_PATTERN;

    public static Class<? extends ICordaPayloads> READ_PAYLOAD_PATTERN;

    public static final boolean SEND_TRACKED = PROPERTIES_CONFIGURATION.getBoolean("SEND_TRACKED");
    public static final boolean PREPARE_WRITE_PAYLOADS = PROPERTIES_CONFIGURATION.getBoolean("PREPARE_WRITE_PAYLOADS");
    public static final boolean SEND_WRITE_ASYNC = PROPERTIES_CONFIGURATION.getBoolean("SEND_WRITE_ASYNC");
    public static final boolean SEND_WRITE_SYNC = PROPERTIES_CONFIGURATION.getBoolean("SEND_WRITE_SYNC");
    public static final boolean LISTENER_AS_THREAD = PROPERTIES_CONFIGURATION.getBoolean("LISTENER_AS_THREAD");
    public static final int DEFAULT_PAGE_NUMBER = PROPERTIES_CONFIGURATION.getInt("DEFAULT_PAGE_NUMBER");
    public static final int DEFAULT_PAGE_SIZE = PROPERTIES_CONFIGURATION.getInt("DEFAULT_PAGE_SIZE");
    public static final List<String> NODE_LIST = PROPERTIES_CONFIGURATION.getList(String.class, "NODE_LIST");
    public static final boolean SET_TRANSACTION_ID = PROPERTIES_CONFIGURATION.getBoolean("SET_TRANSACTION_ID");
    public static final boolean RECEIVE_READ_REQUEST = PROPERTIES_CONFIGURATION.getBoolean("RECEIVE_READ_REQUEST");
    public static final boolean ADD_TRANSACTION_NOTE = PROPERTIES_CONFIGURATION.getBoolean("ADD_TRANSACTION_NOTE");
    public static final boolean PRINT_DEBUG = PROPERTIES_CONFIGURATION.getBoolean("PRINT_DEBUG");
    public static final boolean USE_PREPARED_PROXY = PROPERTIES_CONFIGURATION.getBoolean("USE_PREPARED_PROXY");
    public static final double LISTENER_THRESHOLD = PROPERTIES_CONFIGURATION.getDouble("LISTENER_THRESHOLD");

    public static final long RECONNECTION_SLEEP_TIME = PROPERTIES_CONFIGURATION.getLong("RECONNECTION_SLEEP_TIME");
    public static final int MAX_CONNECTION_RETRIES = PROPERTIES_CONFIGURATION.getInt("MAX_CONNECTION_RETRIES");

    public static final double LISTENER_TOTAL_THRESHOLD = PROPERTIES_CONFIGURATION.getDouble(
            "LISTENER_TOTAL_THRESHOLD");
    public static final int NUMBER_OF_LISTENERS = (PROPERTIES_CONFIGURATION.getInt("NUMBER_OF_LISTENERS") == -1 ? GeneralConfiguration.CLIENT_COUNT :
            PROPERTIES_CONFIGURATION.getInt("NUMBER_OF_LISTENERS"));
    public static final boolean ENABLE_LISTENER = PROPERTIES_CONFIGURATION.getBoolean("ENABLE_LISTENER");
    public static final long TIMEOUT_LISTENER = PROPERTIES_CONFIGURATION.getLong("TIMEOUT_LISTENER");
    public static final TimeUnit TIMEOUT_LISTENER_TIME_UNIT = PROPERTIES_CONFIGURATION.get(TimeUnit.class,
            "TIMEOUT_LISTENER_TIME_UNIT");
    public static final boolean WAIT_FOR_FLOW_DONE = PROPERTIES_CONFIGURATION.getBoolean("WAIT_FOR_FLOW_DONE");
    public static final CloseFlowPoint CLOSE_FLOW_POINT =
            PROPERTIES_CONFIGURATION.get(CloseFlowPoint.class,
                    "CLOSE_FLOW_POINT");

    public static final boolean ENABLE_RATE_LIMITER_FOR_WRITE_PAYLOADS = PROPERTIES_CONFIGURATION.getBoolean(
            "ENABLE_RATE_LIMITER_FOR_WRITE_PAYLOADS");
    public static final List<Double> WRITE_PAYLOADS_PER_SECOND = PROPERTIES_CONFIGURATION.getList(Double.class,
            "WRITE_PAYLOADS_PER_SECOND");
    public static final boolean PREPARE_READ_PAYLOADS = PROPERTIES_CONFIGURATION.getBoolean("PREPARE_READ_PAYLOADS");
    public static final boolean ENABLE_RATE_LIMITER_FOR_READ_PAYLOADS = PROPERTIES_CONFIGURATION.getBoolean(
            "ENABLE_RATE_LIMITER_FOR_READ_PAYLOADS");
    public static final List<Double> READ_PAYLOADS_PER_SECOND = PROPERTIES_CONFIGURATION.getList(Double.class,
            "READ_PAYLOADS_PER_SECOND");
    public static final boolean RETURN_ON_EVENT_DUPLICATE = PROPERTIES_CONFIGURATION.getBoolean(
            "RETURN_ON_EVENT_DUPLICATE");
    public static final boolean SEND_WRITE_REQUESTS = PROPERTIES_CONFIGURATION.getBoolean("SEND_WRITE_REQUESTS");
    public static final boolean SEND_READ_REQUESTS = PROPERTIES_CONFIGURATION.getBoolean("SEND_READ_REQUESTS");
    public static final int STRING_STYLE_MAX_RECURSIVE_DEPTH = PROPERTIES_CONFIGURATION.getInt(
            "STRING_STYLE_MAX_RECURSIVE_DEPTH");
    public static final List<String> EVENT_EXISTS_SUFFIX_LIST = PROPERTIES_CONFIGURATION.getList(String.class,
            "EVENT_EXISTS_SUFFIX_LIST");
    public static final boolean HANDLE_EVENT_SYNCHRONIZED = PROPERTIES_CONFIGURATION.getBoolean(
            "HANDLE_EVENT_SYNCHRONIZED");

    public static final int SORT_ARRAY_LENGTH = PROPERTIES_CONFIGURATION.getInt("SORT_ARRAY_LENGTH");

    public static final int KEY_VALUE_STRING_LENGTH = PROPERTIES_CONFIGURATION.getInt("KEY_VALUE_STRING_LENGTH");

    public static final int LEN_OUTER_LOOP_MEMORY = PROPERTIES_CONFIGURATION.getInt("LEN_OUTER_LOOP_MEMORY");
    public static final int LEN_INNER_LOOP_MEMORY = PROPERTIES_CONFIGURATION.getInt("LEN_INNER_LOOP_MEMORY");
    public static final int FIRST_CHAR_INT_MEMORY = PROPERTIES_CONFIGURATION.getInt("FIRST_CHAR_INT_MEMORY");
    public static final int LENGTH_MEMORY = PROPERTIES_CONFIGURATION.getInt("LENGTH_MEMORY");

    public static final int START_RECURSION = PROPERTIES_CONFIGURATION.getInt("START_RECURSION");
    public static final int END_RECURSION = PROPERTIES_CONFIGURATION.getInt("END_RECURSION");

    public static final int START_LOOP = PROPERTIES_CONFIGURATION.getInt("START_LOOP");
    public static final int END_LOOP = PROPERTIES_CONFIGURATION.getInt("END_LOOP");

    public static final int SIZE_IO = PROPERTIES_CONFIGURATION.getInt("SIZE_IO");
    public static final int RET_LEN_IO = PROPERTIES_CONFIGURATION.getInt("RET_LEN_IO");

    public static final int CHECKING_BALANCE = PROPERTIES_CONFIGURATION.getInt("CHECKING_BALANCE");
    public static final int SAVINGS_BALANCE = PROPERTIES_CONFIGURATION.getInt("SAVINGS_BALANCE");
    public static final int WRITE_CHECK_AMOUNT = PROPERTIES_CONFIGURATION.getInt("WRITE_CHECK_AMOUNT");
    public static final int DEPOSIT_CHECK_AMOUNT = PROPERTIES_CONFIGURATION.getInt("DEPOSIT_CHECK_AMOUNT");
    public static final int TRANSACT_SAVINGS_AMOUNT = PROPERTIES_CONFIGURATION.getInt("TRANSACT_SAVINGS_AMOUNT");
    public static final int SEND_PAYMENT_AMOUNT = PROPERTIES_CONFIGURATION.getInt("SEND_PAYMENT_AMOUNT");
    public static final int SEND_CYCLE = PROPERTIES_CONFIGURATION.getInt("SEND_CYCLE");

    static {
        try {
            WRITE_PAYLOAD_PATTERN = (Class<? extends ICordaPayloads>) Class.forName(PROPERTIES_CONFIGURATION.getString("WRITE_PAYLOAD_PATTERN"));
        } catch (ClassNotFoundException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    static {
        try {
            READ_PAYLOAD_PATTERN = (Class<? extends ICordaPayloads>) Class.forName(PROPERTIES_CONFIGURATION.getString("READ_PAYLOAD_PATTERN"));
        } catch (ClassNotFoundException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    static {
        try {
            WRITE_PAYLOAD = (Class<? extends ICordaWritePayload>) Class.forName(PROPERTIES_CONFIGURATION.getString(
                    "WRITE_PAYLOAD"));
        } catch (ClassNotFoundException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    static {
        try {
            READ_PAYLOAD = (Class<? extends ICordaReadPayload>) Class.forName(PROPERTIES_CONFIGURATION.getString(
                    "READ_PAYLOAD"));
        } catch (ClassNotFoundException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    static {
        GeneralConfiguration.notes = GeneralConfiguration.notes + " | " +
                (PROPERTIES_CONFIGURATION.getString("notes") == null ? "" : PROPERTIES_CONFIGURATION.getString("notes"
                ));
    }

    static {
        boolean potentialInconsistent = false;
        System.out.println("Checking for potential inconsistencies of the configuration...");
        System.out.println("Config potentially consistent: " + !potentialInconsistent);
    }

    private Configuration() {
    }

}
