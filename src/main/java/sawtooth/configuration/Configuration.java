package sawtooth.configuration;

import client.configuration.GeneralConfiguration;
import client.supplements.ExceptionHandler;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;
import sawtooth.connection.ConnectionEnum;
import sawtooth.connection.SocketCreationEnum;
import sawtooth.listener.UpdateMeasureTimeType;
import sawtooth.payload_patterns.ISawtoothPayloads;
import sawtooth.payload_patterns.ITransactionToBatchDispatcher;
import sawtooth.payloads.*;
import sawtooth.sdk.protobuf.ClientEventsSubscribeRequest;
import sawtooth.sdk.protobuf.EventFilter;
import sawtooth.sdk.protobuf.EventSubscription;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public final class Configuration {

    private static final Path CURRENT_RELATIVE_PATH = Paths.get("");
    private static final String CURRENT_ABSOLUTE_PATH = CURRENT_RELATIVE_PATH.toAbsolutePath().toString();
    public static final String FILE_NAME = "sawtoothConfiguration.properties";
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

    public static final List<EventSubscription> EVENT_SUBSCRIPTIONS = new ArrayList<>();
    public static final List<String> EVENT_DECODER_LIST = new ArrayList<>();

    public static Class<? extends ISawtoothPayloads> WRITE_PAYLOAD_PATTERN;

    public static Class<? extends ISawtoothPayloads> READ_PAYLOAD_PATTERN;

    public static final int NUMBER_OF_LISTENERS =
            (PROPERTIES_CONFIGURATION.getInt("NUMBER_OF_LISTENERS") == -1 ? GeneralConfiguration.CLIENT_COUNT :
                    PROPERTIES_CONFIGURATION.getInt("NUMBER_OF_LISTENERS"));

    private static final ClientEventsSubscribeRequest.Builder clientEventsSubscribeRequestBuilder =
            ClientEventsSubscribeRequest.newBuilder();
    public static Class<? extends ITransactionToBatchDispatcher> I_TRANSACTION_TO_BATCH_DISPATCHER;
    public static Class<? extends ISawtoothWritePayload> WRITE_PAYLOAD;
    public static Class<? extends ISawtoothReadPayload> READ_PAYLOAD;
    public static ClientEventsSubscribeRequest SUBSCRIPTIONS;
    // Default: 0 block or timeout
    public static int ZMQ_SOCKET_FLAG_WRITE;
    public static int ZMQ_SOCKET_FLAG_READ;
    public static final ConnectionEnum CONNECTION_TYPE_WRITE = PROPERTIES_CONFIGURATION.get(ConnectionEnum.class,
            "CONNECTION_TYPE_WRITE");
    public static final int NUMBER_OF_BATCHES_PER_CLIENT = PROPERTIES_CONFIGURATION.getInt(
            "NUMBER_OF_BATCHES_PER_CLIENT");
    public static final int NUMBER_OF_TRANSACTIONS_PER_BATCH_PER_CLIENT = PROPERTIES_CONFIGURATION.getInt(
            "NUMBER_OF_TRANSACTIONS_PER_BATCH_PER_CLIENT");
    public static final int NUMBER_OF_TRANSACTION_PAYLOADS_PER_CLIENT =
            NUMBER_OF_BATCHES_PER_CLIENT * NUMBER_OF_TRANSACTIONS_PER_BATCH_PER_CLIENT;
    public static final boolean DECODE_DATA_AS_CBOR = PROPERTIES_CONFIGURATION.getBoolean("DECODE_DATA_AS_CBOR");
    public static final List<String> FAMILY_NAMES = PROPERTIES_CONFIGURATION.getList(String.class, "FAMILY_NAMES");
    public static final List<String> FAMILY_VERSIONS = PROPERTIES_CONFIGURATION.getList(String.class,
            "FAMILY_VERSIONS");
    public static final String TP_PREFIX = PROPERTIES_CONFIGURATION.getString("TP_PREFIX");
    public static final boolean USE_POST_FOR_BATCH_CHECK = PROPERTIES_CONFIGURATION.getBoolean(
            "USE_POST_FOR_BATCH_CHECK");
    public static final boolean SEND_WRITE_ASYNC = PROPERTIES_CONFIGURATION.getBoolean("SEND_WRITE_ASYNC");
    public static final int CHECK_COMMITTED_REPOLLS = PROPERTIES_CONFIGURATION.getInt("CHECK_COMMITTED_REPOLLS");
    public static final double COMMIT_BATCHES_THRESHOLD = PROPERTIES_CONFIGURATION.getDouble(
            "COMMIT_BATCHES_THRESHOLD");
    public static final long BATCH_CHECK_REPOLL_INTERVAL = PROPERTIES_CONFIGURATION.getLong(
            "BATCH_CHECK_REPOLL_INTERVAL");
    public static final String QUEUE_FULL_RESUBMIT_INTERVAL = PROPERTIES_CONFIGURATION.getString("QUEUE_FULL_RESUBMIT_INTERVAL");
    public static final boolean CHECK_BATCH_STATUS = PROPERTIES_CONFIGURATION.getBoolean("CHECK_BATCH_STATUS");
    public static final boolean ENABLE_LISTENER = PROPERTIES_CONFIGURATION.getBoolean("ENABLE_LISTENER");
    public static final boolean PREPARE_WRITE_PAYLOADS = PROPERTIES_CONFIGURATION.getBoolean("PREPARE_WRITE_PAYLOADS");
    public static final boolean USE_PREPARED_WRITE_WEBSOCKET = PROPERTIES_CONFIGURATION.getBoolean(
            "USE_PREPARED_WRITE_WEBSOCKET");
    public static final boolean WEBSOCKET_LISTENER = PROPERTIES_CONFIGURATION.getBoolean("WEBSOCKET_LISTENER");
    public static final boolean ZMQ_LISTENER = PROPERTIES_CONFIGURATION.getBoolean("ZMQ_LISTENER");
    public static final boolean LISTENER_AS_THREAD = PROPERTIES_CONFIGURATION.getBoolean("LISTENER_AS_THREAD");
    public static final boolean DISPATCH_TRANSACTIONS_TO_BATCHES = PROPERTIES_CONFIGURATION.getBoolean(
            "DISPATCH_TRANSACTIONS_TO_BATCHES");
    public static final boolean DISCONNECT_LISTENERS = PROPERTIES_CONFIGURATION.getBoolean("DISCONNECT_LISTENERS");
    public static final int RESEND_TIMES_UPON_ERROR_WRITE = PROPERTIES_CONFIGURATION.getInt(
            "RESEND_TIMES_UPON_ERROR_WRITE");
    public static final long READ_ZMQ_SLEEP_TIME = PROPERTIES_CONFIGURATION.getLong("READ_ZMQ_SLEEP_TIME");
    public static final long ZMQ_READ_TIMEOUT = PROPERTIES_CONFIGURATION.getLong("ZMQ_READ_TIMEOUT");
    public static final int MAX_CONNECTION_RETRIES = PROPERTIES_CONFIGURATION.getInt("MAX_CONNECTION_RETRIES");
    public static final long WRITE_ZMQ_SLEEP_TIME = PROPERTIES_CONFIGURATION.getLong("WRITE_ZMQ_SLEEP_TIME");
    public static final boolean SEND_BATCH_BY_BATCH = PROPERTIES_CONFIGURATION.getBoolean("SEND_BATCH_BY_BATCH");
    public static final boolean SEND_TRANSACTION_BY_TRANSACTION = PROPERTIES_CONFIGURATION.getBoolean(
            "SEND_TRANSACTION_BY_TRANSACTION");
    public static final long LISTENER_SLEEP_TIME = PROPERTIES_CONFIGURATION.getLong("LISTENER_SLEEP_TIME");
    public static final UpdateMeasureTimeType UPDATE_MEASURE_TIME =
            PROPERTIES_CONFIGURATION.get(UpdateMeasureTimeType.class, "UPDATE_MEASURE_TIME");
    public static final double LISTENER_THRESHOLD = PROPERTIES_CONFIGURATION.getDouble("LISTENER_THRESHOLD");
    public static final String[] VALIDATORS_TO_SEND_TRANSACTIONS_TO_WEBSOCKET =
            PROPERTIES_CONFIGURATION.getStringArray("VALIDATORS_TO_SEND_TRANSACTIONS_TO_WEBSOCKET");
    public static final String[] VALIDATORS_TO_SUBSCRIBE_TO_WS = PROPERTIES_CONFIGURATION.getStringArray(
            "VALIDATORS_TO_SUBSCRIBE_TO_WS");
    public static final String[] VALIDATORS_TO_SEND_TRANSACTIONS_TO_ZMQ =
            PROPERTIES_CONFIGURATION.getStringArray("VALIDATORS_TO_SEND_TRANSACTIONS_TO_ZMQ");
    public static final int TIMEOUT_BATCH_STATUS = PROPERTIES_CONFIGURATION.getInt("TIMEOUT_BATCH_STATUS");
    public static final boolean ENABLE_TRACE = PROPERTIES_CONFIGURATION.getBoolean("ENABLE_TRACE");
    public static final String[] SUBSCRIPTION_JSON_STRINGS = PROPERTIES_CONFIGURATION.getStringArray(
            "SUBSCRIPTION_JSON_STRINGS");
    public static final String[] UNSUBSCRIPTION_JSON_STRINGS = PROPERTIES_CONFIGURATION.getStringArray(
            "UNSUBSCRIPTION_JSON_STRINGS");
    public static final boolean ENABLE_DEBUGGING = PROPERTIES_CONFIGURATION.getBoolean("ENABLE_DEBUGGING");
    public static final String ZMQ_CURVE_PUBLIC_KEY = PROPERTIES_CONFIGURATION.getString("ZMQ_CURVE_PUBLIC_KEY");
    public static final String ZMQ_CURVE_PRIVATE_KEY = PROPERTIES_CONFIGURATION.getString("ZMQ_CURVE_PRIVATE_KEY");
    public static final boolean SEND_PING_RESPONSES = PROPERTIES_CONFIGURATION.getBoolean("SEND_PING_RESPONSES");
    // Usually 1 is sufficient
    public static final int ZMQ_IO_THREADS = PROPERTIES_CONFIGURATION.getInt("ZMQ_IO_THREADS");
    // Default: -1 OS default
    public static final int ZMQ_TCP_KEEP_ALIVE_COUNT = PROPERTIES_CONFIGURATION.getInt("ZMQ_TCP_KEEP_ALIVE_COUNT");
    // Default: -1 OS default
    public static final int ZMQ_TCP_KEEP_ALIVE = PROPERTIES_CONFIGURATION.getInt("ZMQ_TCP_KEEP_ALIVE");
    // Default: -1 infinite
    public static final int ZMQ_RECEIVE_TIMEOUT = PROPERTIES_CONFIGURATION.getInt("ZMQ_RECEIVE_TIMEOUT");
    public static final int ZMQ_SEND_TIMEOUT = PROPERTIES_CONFIGURATION.getInt("ZMQ_SEND_TIMEOUT");
    public static final int ZMQ_RECEIVE_BUFFER_SIZE = PROPERTIES_CONFIGURATION.getInt("ZMQ_RECEIVE_BUFFER_SIZE");
    public static final int ZMQ_SEND_BUFFER_SIZE = PROPERTIES_CONFIGURATION.getInt("ZMQ_SEND_BUFFER_SIZE");
    public static final boolean RECEIVE_READ_REQUEST = PROPERTIES_CONFIGURATION.getBoolean("RECEIVE_READ_REQUEST");
    public static final long TIMEOUT_LISTENER = PROPERTIES_CONFIGURATION.getLong("TIMEOUT_LISTENER");
    public static final TimeUnit TIMEOUT_LISTENER_TIME_UNIT = PROPERTIES_CONFIGURATION.get(TimeUnit.class,
            "TIMEOUT_LISTENER_TIME_UNIT");
    public static final boolean ENABLE_RATE_LIMITER_FOR_WRITE_PAYLOADS = PROPERTIES_CONFIGURATION.getBoolean(
            "ENABLE_RATE_LIMITER_FOR_WRITE_PAYLOADS");
    public static final List<Double> WRITE_PAYLOADS_PER_SECOND = PROPERTIES_CONFIGURATION.getList(Double.class,
            "WRITE_PAYLOADS_PER_SECOND");
    public static final List<Double> READ_PAYLOADS_PER_SECOND = PROPERTIES_CONFIGURATION.getList(Double.class,
            "READ_PAYLOADS_PER_SECOND");
    public static final ConnectionEnum CONNECTION_TYPE_READ = PROPERTIES_CONFIGURATION.get(ConnectionEnum.class,
            "CONNECTION_TYPE_READ");
    public static final boolean USE_PREPARED_READ_WEBSOCKET = PROPERTIES_CONFIGURATION.getBoolean(
            "USE_PREPARED_READ_WEBSOCKET");
    public static final boolean PREPARE_READ_PAYLOADS = PROPERTIES_CONFIGURATION.getBoolean(
            "PREPARE_READ_PAYLOADS");
    public static final int READ_REQUESTS = PROPERTIES_CONFIGURATION.getInt("READ_REQUESTS");
    public static final boolean ENABLE_RATE_LIMITER_FOR_READ_PAYLOADS = PROPERTIES_CONFIGURATION.getBoolean(
            "ENABLE_RATE_LIMITER_FOR_READ_PAYLOADS");
    public static final boolean RETURN_ON_EVENT_DUPLICATE = PROPERTIES_CONFIGURATION.getBoolean(
            "RETURN_ON_EVENT_DUPLICATE");
    public static final boolean DECODE_DATA_AS_CBOR_UPDATE_START_TIME = PROPERTIES_CONFIGURATION.getBoolean(
            "DECODE_DATA_AS_CBOR_UPDATE_START_TIME");
    public static final boolean DECODE_DATA_AS_CBOR_LISTENER = PROPERTIES_CONFIGURATION.getBoolean(
            "DECODE_DATA_AS_CBOR_LISTENER");
    public static final boolean DECODE_DATA_AS_CBOR_READ = PROPERTIES_CONFIGURATION.getBoolean(
            "DECODE_DATA_AS_CBOR_READ");
    public static final double LISTENER_TOTAL_THRESHOLD = PROPERTIES_CONFIGURATION.getDouble(
            "LISTENER_TOTAL_THRESHOLD");
    public static final long WAIT_TIME_AFTER_CHECK_WRITE_REQUEST = PROPERTIES_CONFIGURATION.getLong(
            "WAIT_TIME_AFTER_CHECK_WRITE_REQUEST");
    public static final int WRITE_POLL_RETRIES = PROPERTIES_CONFIGURATION.getInt("WRITE_POLL_RETRIES");
    public static final boolean RETRY_WRITE_POLL_ON_FAIL = PROPERTIES_CONFIGURATION.getBoolean(
            "RETRY_WRITE_POLL_ON_FAIL");
    public static final boolean SEND_READ_REQUESTS = PROPERTIES_CONFIGURATION.getBoolean("SEND_READ_REQUESTS");
    public static final boolean SEND_WRITE_REQUESTS = PROPERTIES_CONFIGURATION.getBoolean("SEND_WRITE_REQUESTS");
    public static final long WEBSOCKET_RECONNECTION_SLEEP_TIME = PROPERTIES_CONFIGURATION.getLong(
            "WEBSOCKET_RECONNECTION_SLEEP_TIME");
    public static final int RESEND_TIMES_UPON_ERROR_READ = PROPERTIES_CONFIGURATION.getInt(
            "RESEND_TIMES_UPON_ERROR_READ");
    public static final SocketCreationEnum SOCKET_CREATION_ENUM =
            PROPERTIES_CONFIGURATION.get(SocketCreationEnum.class, "SOCKET_CREATION_ENUM");
    public static final List<String> EVENT_EXISTS_SUFFIX_LIST = PROPERTIES_CONFIGURATION.getList(String.class,
            "EVENT_EXISTS_SUFFIX_LIST");
    public static final boolean HANDLE_EVENT_SYNCHRONIZED = PROPERTIES_CONFIGURATION.getBoolean(
            "HANDLE_EVENT_SYNCHRONIZED");
    public static TpEnum TP_ENUM = PROPERTIES_CONFIGURATION.get(TpEnum.class, "TP_ENUM");

    public static final boolean ENABLE_BLOCK_STATISTICS = PROPERTIES_CONFIGURATION.getBoolean(
            "ENABLE_BLOCK_STATISTICS");
    public static final boolean USE_ZMQ_FOR_BLOCK_STATISTICS = PROPERTIES_CONFIGURATION.getBoolean(
            "USE_ZMQ_FOR_BLOCK_STATISTICS");

    public static final long TIMEOUT_TRANSACTION = PROPERTIES_CONFIGURATION.getInt("TIMEOUT_TRANSACTION");
    public static final TimeUnit TIMEOUT_UNIT_TRANSACTION = PROPERTIES_CONFIGURATION.get(TimeUnit.class, "TIMEOUT_UNIT_TRANSACTION");
    public static final boolean DROP_ON_TIMEOUT = PROPERTIES_CONFIGURATION.getBoolean("DROP_ON_TIMEOUT");
    public static final boolean DROP_ON_ERROR_4 = PROPERTIES_CONFIGURATION.getBoolean("DROP_ON_ERROR_4");

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

    public static final boolean SET_PAYLOAD_DEPENDING_ON_PATTERN = PROPERTIES_CONFIGURATION.getBoolean("SET_PAYLOAD_DEPENDING_ON_PATTERN");

    static {
        try {
            ZMQ_SOCKET_FLAG_WRITE = (int) zmq.ZMQ.class.getDeclaredField(PROPERTIES_CONFIGURATION.getString(
                    "ZMQ_SOCKET_FLAG_WRITE")).get(null);
        } catch (IllegalAccessException | NoSuchFieldException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    static {
        try {
            ZMQ_SOCKET_FLAG_READ = (int) zmq.ZMQ.class.getDeclaredField(PROPERTIES_CONFIGURATION.getString(
                    "ZMQ_SOCKET_FLAG_READ")).get(null);
        } catch (IllegalAccessException | NoSuchFieldException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    static {
        try {
            I_TRANSACTION_TO_BATCH_DISPATCHER =
                    (Class<? extends ITransactionToBatchDispatcher>) Class.forName(PROPERTIES_CONFIGURATION.getString(
                            "I_TRANSACTION_TO_BATCH_DISPATCHER"));
        } catch (ClassNotFoundException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    static {
        try {
            WRITE_PAYLOAD_PATTERN =
                    (Class<? extends ISawtoothPayloads>) Class.forName(PROPERTIES_CONFIGURATION.getString(
                            "WRITE_PAYLOAD_PATTERN"));
        } catch (ClassNotFoundException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    static {
        try {
            READ_PAYLOAD_PATTERN =
                    (Class<? extends ISawtoothPayloads>) Class.forName(PROPERTIES_CONFIGURATION.getString(
                            "READ_PAYLOAD_PATTERN"));
        } catch (ClassNotFoundException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    static {
        if(!SET_PAYLOAD_DEPENDING_ON_PATTERN) {
            try {
                WRITE_PAYLOAD = (Class<? extends ISawtoothWritePayload>) Class.forName(PROPERTIES_CONFIGURATION.getString(
                        "WRITE_PAYLOAD"));
            } catch (ClassNotFoundException ex) {
                ExceptionHandler.logException(ex);
            }
        }
    }

    static {
        try {
            READ_PAYLOAD = (Class<? extends ISawtoothReadPayload>) Class.forName(PROPERTIES_CONFIGURATION.getString(
                    "READ_PAYLOAD"));
        } catch (ClassNotFoundException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    static {
        if (SET_PAYLOAD_DEPENDING_ON_PATTERN) {
            try {
                Class<? extends ISawtoothPayloads> write_payload_pattern =
                        (Class<? extends ISawtoothPayloads>) Class.forName(PROPERTIES_CONFIGURATION.getString(
                                "WRITE_PAYLOAD_PATTERN"));
                String[] split = write_payload_pattern.getPackage().getName().split("\\.");
                if ("smallbank".equals(split[split.length - 1])) {
                    WRITE_PAYLOAD = GeneralCborTpSbPayload.class;
                } else if ("keyvalue".equals(split[split.length - 1])) {
                    WRITE_PAYLOAD = GeneralCborTpKeyValuePayload.class;
                } else if ("io".equals(split[split.length - 1])) {
                    WRITE_PAYLOAD = GeneralCborTpIOPayload.class;
                } else {
                    System.out.println("Using default no address payload");
                    WRITE_PAYLOAD = GeneralCborNoAddressTpPayload.class;
                }
            } catch (ClassNotFoundException ex) {
                ExceptionHandler.logException(ex);
            }
        }
    }

    static {
        for (final String subscription : PROPERTIES_CONFIGURATION.getList(String.class,
                "EVENT_SUBSCRIPTIONS")) {
            EVENT_SUBSCRIPTIONS.add(geDefaultSubscription(subscription));
        }
    }

    static {
        for (final String eventToDecode : PROPERTIES_CONFIGURATION.getList(String.class,
                "EVENT_DECODER_LIST")) {
            EVENT_DECODER_LIST.add(geDefaultSubscription(eventToDecode).getEventType());
        }
    }

    static {
        for (final String eventToDecode : PROPERTIES_CONFIGURATION.getList(String.class,
                "SUBSCRIPTIONS")) {
            clientEventsSubscribeRequestBuilder.addSubscriptions((geDefaultSubscription(eventToDecode)));
        }
        SUBSCRIPTIONS = clientEventsSubscribeRequestBuilder.build();
    }

    static {
        GeneralConfiguration.notes = GeneralConfiguration.notes + " | " +
                (PROPERTIES_CONFIGURATION.getString("notes") == null ? "" : PROPERTIES_CONFIGURATION.getString("notes"
                ));
    }

    public static EventSubscription geDefaultSubscription(final String event) {
        return EventSubscription.newBuilder().setEventType(
                event).addFilters(EventFilter.newBuilder().setFilterType(EventFilter.FilterType.REGEX_ALL).build()).build();
    }

}
