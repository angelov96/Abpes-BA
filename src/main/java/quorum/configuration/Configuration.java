package quorum.configuration;

import client.configuration.GeneralConfiguration;
import client.supplements.ExceptionHandler;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.web3j.abi.TypeReference;
import org.web3j.protocol.core.DefaultBlockParameter;
import org.web3j.protocol.core.DefaultBlockParameterName;
import quorum.payload_patterns.IQuorumPayloads;
import quorum.payloads.IQuorumReadPayload;
import quorum.payloads.IQuorumWritePayload;

import java.io.File;
import java.math.BigInteger;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public final class Configuration {

    private static final Path CURRENT_RELATIVE_PATH = Paths.get("");
    private static final String CURRENT_ABSOLUTE_PATH = CURRENT_RELATIVE_PATH.toAbsolutePath().toString();
    public static final String FILE_NAME = "quorumConfiguration.properties";
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

    public static Class<? extends IQuorumWritePayload> WRITE_PAYLOAD;
    public static Class<? extends IQuorumReadPayload> READ_PAYLOAD;
    public static BigInteger DEFAULT_GAS_PRICE;

    public static final int START_ADDRESS = PROPERTIES_CONFIGURATION.getInt("START_ADDRESS");
    public static final int END_ADDRESS = PROPERTIES_CONFIGURATION.getInt("END_ADDRESS");

    public static final long DATABASE_SLEEP_TIME = PROPERTIES_CONFIGURATION.getLong("DATABASE_SLEEP_TIME");

    public static List<TypeReference<?>> DEFAULT_EVENT_TYPE_REFERENCE_LIST = new ArrayList<>();
    public static final DefaultBlockParameter DEFAULT_BLOCK_PARAMETER =
            PROPERTIES_CONFIGURATION.get(DefaultBlockParameterName.class, "DEFAULT_BLOCK_PARAMETER");
    public static final boolean SEND_WRITE_REQUESTS = PROPERTIES_CONFIGURATION.getBoolean("SEND_WRITE_REQUESTS");
    public static final boolean SEND_READ_REQUESTS = PROPERTIES_CONFIGURATION.getBoolean("SEND_READ_REQUESTS");
    public static final File ADDRESS_OR_WALLET_FILE = new File(PROPERTIES_CONFIGURATION.getString(
            "ADDRESS_OR_WALLET_FILE"));
    public static final List<String> CONTRACT_ADDRESS_LIST = PROPERTIES_CONFIGURATION.getList(String.class,
            "CONTRACT_ADDRESS_LIST");
    public static final boolean OVERWRITE_READ_ADDRESS = PROPERTIES_CONFIGURATION.getBoolean("OVERWRITE_READ_ADDRESS");
    public static final boolean DISTRIBUTED_NONCE_HANDLING = PROPERTIES_CONFIGURATION.getBoolean("DISTRIBUTED_NONCE_HANDLING");
    public static final boolean INCLUDE_RAW_RESPONSES = PROPERTIES_CONFIGURATION.getBoolean("INCLUDE_RAW_RESPONSES");
    public static final int NUMBER_OF_TRANSACTIONS_PER_CLIENT = PROPERTIES_CONFIGURATION.getInt(
            "NUMBER_OF_TRANSACTIONS_PER_CLIENT");
    public static final BigInteger DEFAULT_GAS_LIMIT = PROPERTIES_CONFIGURATION.getBigInteger("DEFAULT_GAS_LIMIT");
    public static final BigInteger DEFAULT_UNLOCK_DURATION = PROPERTIES_CONFIGURATION.getBigInteger(
            "DEFAULT_UNLOCK_DURATION");

    public static final int NUMBER_OF_LISTENERS =
            (PROPERTIES_CONFIGURATION.getInt("NUMBER_OF_LISTENERS") == -1 ? GeneralConfiguration.CLIENT_COUNT :
                    PROPERTIES_CONFIGURATION.getInt("NUMBER_OF_LISTENERS"));

    public static final long TIMEOUT_TRANSACTION = PROPERTIES_CONFIGURATION.getInt("TIMEOUT_TRANSACTION");
    public static final TimeUnit TIMEOUT_UNIT_TRANSACTION = PROPERTIES_CONFIGURATION.get(TimeUnit.class, "TIMEOUT_UNIT_TRANSACTION");
    public static final boolean DROP_ON_TIMEOUT = PROPERTIES_CONFIGURATION.getBoolean("DROP_ON_TIMEOUT");

    public static Class<? extends IQuorumPayloads> WRITE_PAYLOAD_PATTERN;

    public static Class<? extends IQuorumPayloads> READ_PAYLOAD_PATTERN;

    public static final int LOGIN_RETRIES = PROPERTIES_CONFIGURATION.getInt("LOGIN_RETRIES");
    public static final int TRANSACTION_RECEIPT_RETRIES = PROPERTIES_CONFIGURATION.getInt(
            "TRANSACTION_RECEIPT_RETRIES");
    public static final int TRANSACTION_RECEIPT_SLEEP = PROPERTIES_CONFIGURATION.getInt("TRANSACTION_RECEIPT_SLEEP");
    public static final boolean SEND_WRITE_ASYNC = PROPERTIES_CONFIGURATION.getBoolean("SEND_WRITE_ASYNC");
    public static final boolean SEND_WRITE_SYNC = PROPERTIES_CONFIGURATION.getBoolean("SEND_WRITE_SYNC");
    public static final boolean SEND_READ_ASYNC = PROPERTIES_CONFIGURATION.getBoolean("SEND_READ_ASYNC");
    public static final boolean SEND_READ_SYNC = PROPERTIES_CONFIGURATION.getBoolean("SEND_READ_SYNC");
    public static final boolean DEBUG_SENT_TRANSACTION = PROPERTIES_CONFIGURATION.getBoolean("DEBUG_SENT_TRANSACTION");
    public static final boolean SIGN_RAW_LOCAL = PROPERTIES_CONFIGURATION.getBoolean("SIGN_RAW_LOCAL");
    public static final boolean SIGN_RAW_WEB3J = PROPERTIES_CONFIGURATION.getBoolean("SIGN_RAW_WEB3J");
    public static final boolean SEND_RAW = PROPERTIES_CONFIGURATION.getBoolean("SEND_RAW");
    public static final boolean CALCULATE_TX_HASH = PROPERTIES_CONFIGURATION.getBoolean("CALCULATE_TX_HASH");
    public static final boolean DECODE_READ_DATA = PROPERTIES_CONFIGURATION.getBoolean("DECODE_READ_DATA");
    public static final boolean DIRECT_READ_AFTER_WRITE = PROPERTIES_CONFIGURATION.getBoolean(
            "DIRECT_READ_AFTER_WRITE");
    public static final int RESEND_TIMES_UPON_ERROR_WRITE = PROPERTIES_CONFIGURATION.getInt(
            "RESEND_TIMES_UPON_ERROR_WRITE");
    public static final long TIMEOUT_LISTENER = PROPERTIES_CONFIGURATION.getLong("TIMEOUT_LISTENER");
    public static final TimeUnit TIMEOUT_LISTENER_TIME_UNIT = PROPERTIES_CONFIGURATION.get(TimeUnit.class,
            "TIMEOUT_LISTENER_TIME_UNIT");
    public static final boolean MEASURE_BY_RECEIPT = PROPERTIES_CONFIGURATION.getBoolean("MEASURE_BY_RECEIPT");
    public static final boolean LISTENER_AS_THREAD = PROPERTIES_CONFIGURATION.getBoolean("LISTENER_AS_THREAD");
    public static final boolean NON_RAW_NULL_NONCE_HANDLING = PROPERTIES_CONFIGURATION.getBoolean(
            "NON_RAW_NULL_NONCE_HANDLING");
    public static final int MAX_CONNECTION_RETRIES = PROPERTIES_CONFIGURATION.getInt("MAX_CONNECTION_RETRIES");
    public static final int DEFAULT_BLOCK_TIME = PROPERTIES_CONFIGURATION.getInt("DEFAULT_BLOCK_TIME");
    public static final String WALLET_PATH_PREFIX = PROPERTIES_CONFIGURATION.getString("WALLET_PATH_PREFIX");
    public static final String WALLET_ENDING = PROPERTIES_CONFIGURATION.getString("WALLET_ENDING");
    public static final boolean DEBUG_TRANSACTION_COUNT = PROPERTIES_CONFIGURATION.getBoolean(
            "DEBUG_TRANSACTION_COUNT");
    public static final boolean PREPARE_WRITE_PAYLOADS = PROPERTIES_CONFIGURATION.getBoolean("PREPARE_WRITE_PAYLOADS");
    public static final boolean ENABLE_LISTENER = PROPERTIES_CONFIGURATION.getBoolean("ENABLE_LISTENER");
    public static final boolean UNREGISTER_LISTENERS = PROPERTIES_CONFIGURATION.getBoolean("UNREGISTER_LISTENERS");
    public static final boolean RECEIVE_READ_REQUEST = PROPERTIES_CONFIGURATION.getBoolean("RECEIVE_READ_REQUEST");
    public static final boolean USE_PREPARED_WEBSOCKET_SERVICE_WRITE = PROPERTIES_CONFIGURATION.getBoolean(
            "USE_PREPARED_WEBSOCKET_SERVICE_WRITE");
    public static final boolean USE_PREPARED_WEB3J_WRITE = PROPERTIES_CONFIGURATION.getBoolean(
            "USE_PREPARED_WEB3J_WRITE");
    public static final double LISTENER_THRESHOLD = PROPERTIES_CONFIGURATION.getDouble("LISTENER_THRESHOLD");
    public static final boolean ENABLE_RATE_LIMITER_FOR_WRITE_PAYLOADS = PROPERTIES_CONFIGURATION.getBoolean(
            "ENABLE_RATE_LIMITER_FOR_WRITE_PAYLOADS");
    public static final List<Double> WRITE_PAYLOADS_PER_SECOND = PROPERTIES_CONFIGURATION.getList(Double.class,
            "WRITE_PAYLOADS_PER_SECOND");
    public static final boolean USE_PREPARED_WEB3J_READ = PROPERTIES_CONFIGURATION.getBoolean(
            "USE_PREPARED_WEB3J_READ");
    public static final boolean USE_PREPARED_WEBSOCKET_SERVICE_READ = PROPERTIES_CONFIGURATION.getBoolean(
            "USE_PREPARED_WEBSOCKET_SERVICE_READ");
    public static final boolean ENABLE_RATE_LIMITER_FOR_READ_PAYLOADS = PROPERTIES_CONFIGURATION.getBoolean(
            "ENABLE_RATE_LIMITER_FOR_READ_PAYLOADS");
    public static final List<Double> READ_PAYLOADS_PER_SECOND = PROPERTIES_CONFIGURATION.getList(Double.class,
            "READ_PAYLOADS_PER_SECOND");
    public static final boolean PREPARE_READ_PAYLOADS = PROPERTIES_CONFIGURATION.getBoolean("PREPARE_READ_PAYLOADS");
    public static final boolean RETURN_ON_EVENT_DUPLICATE = PROPERTIES_CONFIGURATION.getBoolean(
            "RETURN_ON_EVENT_DUPLICATE");
    public static final double LISTENER_TOTAL_THRESHOLD = PROPERTIES_CONFIGURATION.getDouble(
            "LISTENER_TOTAL_THRESHOLD");
    public static final long WEBSOCKET_RECONNECTION_SLEEP_TIME = PROPERTIES_CONFIGURATION.getLong(
            "WEBSOCKET_RECONNECTION_SLEEP_TIME");
    public static final int RESEND_TIMES_UPON_ERROR_READ = PROPERTIES_CONFIGURATION.getInt(
            "RESEND_TIMES_UPON_ERROR_READ");
    public static final List<String> EVENT_EXISTS_SUFFIX_LIST = PROPERTIES_CONFIGURATION.getList(String.class,
            "EVENT_EXISTS_SUFFIX_LIST");
    public static final boolean HANDLE_EVENT_SYNCHRONIZED = PROPERTIES_CONFIGURATION.getBoolean(
            "HANDLE_EVENT_SYNCHRONIZED");
    public static final boolean ENABLE_BLOCK_STATISTICS = PROPERTIES_CONFIGURATION.getBoolean("ENABLE_BLOCK_STATISTICS");

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
            WRITE_PAYLOAD_PATTERN = (Class<? extends IQuorumPayloads>) Class.forName(PROPERTIES_CONFIGURATION.getString("WRITE_PAYLOAD_PATTERN"));
        } catch (ClassNotFoundException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    static {
        try {
            READ_PAYLOAD_PATTERN = (Class<? extends IQuorumPayloads>) Class.forName(PROPERTIES_CONFIGURATION.getString("READ_PAYLOAD_PATTERN"));
        } catch (ClassNotFoundException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    static {
        try {
            WRITE_PAYLOAD = (Class<? extends IQuorumWritePayload>) Class.forName(PROPERTIES_CONFIGURATION.getString(
                    "WRITE_PAYLOAD"));
        } catch (ClassNotFoundException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    static {
        try {
            READ_PAYLOAD = (Class<? extends IQuorumReadPayload>) Class.forName(PROPERTIES_CONFIGURATION.getString(
                    "READ_PAYLOAD"));
        } catch (ClassNotFoundException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    static {
        List<String> default_event_type_reference_list = PROPERTIES_CONFIGURATION.getList(String.class,
                "DEFAULT_EVENT_TYPE_REFERENCE_LIST");
        try {

            for (final String default_event_type_reference : default_event_type_reference_list) {
                Class clazz = Class.forName(default_event_type_reference);
                DEFAULT_EVENT_TYPE_REFERENCE_LIST.add(
                        TypeReference.create(
                                clazz
                        )
                );
            }
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

    static {
        try {
            DEFAULT_GAS_PRICE = (BigInteger) BigInteger.class.getDeclaredField(PROPERTIES_CONFIGURATION.getString(
                    "DEFAULT_GAS_PRICE")).get(null);
        } catch (IllegalAccessException | NoSuchFieldException ex) {
            ExceptionHandler.logException(ex);
        }
    }

}
