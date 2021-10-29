package graphene.configuration;

import client.configuration.GeneralConfiguration;
import client.supplements.ExceptionHandler;
import client.utils.ThreadPoolFactoryFactoryFacade;
import cy.agorise.graphenej.Asset;
import cy.agorise.graphenej.RPC;
import cy.agorise.graphenej.Util;
import graphene.listener.UpdateMeasureTimeType;
import graphene.payload_patterns.IGraphenePayloads;
import graphene.payload_patterns.IOperationToTransactionDispatcher;
import graphene.payloads.IGrapheneReadPayload;
import graphene.payloads.IGrapheneWritePayload;
import graphene.payloads.ReadPayloadType;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractQueue;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public final class Configuration {

    private static final Path CURRENT_RELATIVE_PATH = Paths.get("");
    private static final String CURRENT_ABSOLUTE_PATH = CURRENT_RELATIVE_PATH.toAbsolutePath().toString();
    public static final String FILE_NAME = "grapheneConfiguration.properties";
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

    public static final Map<String, String> ACCT_ID_PRIVATE_KEY_MAP = new LinkedHashMap<>();;
    public static Class<? extends IGrapheneWritePayload> WRITE_PAYLOAD;
    public static Class<? extends IGrapheneReadPayload> READ_PAYLOAD;
    public static String BROADCAST_TYPE;
    public static Class<? extends IOperationToTransactionDispatcher> I_OPERATION_TO_TRANSACTION_DISPATCHER;
    public static Class<? extends AbstractQueue> ABSTRACT_QUEUE;

    public static final int NUMBER_OF_LISTENERS =
            (PROPERTIES_CONFIGURATION.getInt("NUMBER_OF_LISTENERS") == -1 ? GeneralConfiguration.CLIENT_COUNT :
                    PROPERTIES_CONFIGURATION.getInt("NUMBER_OF_LISTENERS"));

    public static final String CHAIN_ID = PROPERTIES_CONFIGURATION.getString("CHAIN_ID");
    public static final byte[] CHAIN_ID_BYTES = Util.hexToBytes(CHAIN_ID);
    public static final boolean SET_SUBSCRIBE_CALLBACK = PROPERTIES_CONFIGURATION.getBoolean("SET_SUBSCRIBE_CALLBACK");
    public static final boolean SET_PENDING_TRANSACTION_CALLBACK = PROPERTIES_CONFIGURATION.getBoolean(
            "SET_PENDING_TRANSACTION_CALLBACK");
    public static final boolean SET_BLOCK_APPLIED_CALLBACK = PROPERTIES_CONFIGURATION.getBoolean(
            "SET_BLOCK_APPLIED_CALLBACK");
    public static final boolean SET_AUTO_SUBSCRIPTION = PROPERTIES_CONFIGURATION.getBoolean("SET_AUTO_SUBSCRIPTION");
    public static final boolean DISPATCH_OPERATIONS_TO_TRANSACTIONS = PROPERTIES_CONFIGURATION.getBoolean(
            "DISPATCH_OPERATIONS_TO_TRANSACTIONS");
    public static final int NUMBER_OF_OPERATIONS_PER_TRANSACTION = PROPERTIES_CONFIGURATION.getInt(
            "NUMBER_OF_OPERATIONS_PER_TRANSACTION");
    public static final int NUMBER_OF_TRANSACTIONS_PER_CLIENT = PROPERTIES_CONFIGURATION.getInt(
            "NUMBER_OF_TRANSACTIONS_PER_CLIENT");
    public static final int NUMBER_OF_TRANSACTION_PAYLOADS_PER_CLIENT =
            NUMBER_OF_OPERATIONS_PER_TRANSACTION * NUMBER_OF_TRANSACTIONS_PER_CLIENT;
    public static final boolean USE_TRANSACTION_BUILDER = PROPERTIES_CONFIGURATION.getBoolean(
            "USE_TRANSACTION_BUILDER");
    public static final boolean IS_TRANSACTION_EXECUTED_BY_WALLET = PROPERTIES_CONFIGURATION.getBoolean(
            "IS_TRANSACTION_EXECUTED_BY_WALLET");
    public static final boolean TRANSACTION_BUILDER_SIGN_WITH_INSTANT_BROADCAST =
            PROPERTIES_CONFIGURATION.getBoolean("TRANSACTION_BUILDER_SIGN_WITH_INSTANT_BROADCAST");
    public static final boolean TRANSACTION_BUILDER_SIGN_BROADCAST = PROPERTIES_CONFIGURATION.getBoolean(
            "TRANSACTION_BUILDER_SIGN_BROADCAST");
    public static final String ADDRESS_PREFIX = PROPERTIES_CONFIGURATION.getString("ADDRESS_PREFIX");
    public static final int PREFIX_LENGTH = ADDRESS_PREFIX.length();
    public static final String RPC_USERNAME_NODE =
            PROPERTIES_CONFIGURATION.getString("RPC_USERNAME_NODE").equals("") ? null :
            PROPERTIES_CONFIGURATION.getString("RPC_USERNAME_NODE");
    public static final String RPC_PASSWORD_NODE =
            PROPERTIES_CONFIGURATION.getString("RPC_PASSWORD_NODE").equals("") ? null :
            PROPERTIES_CONFIGURATION.getString("RPC_PASSWORD_NODE");
    public static final List<String> ERROR_MESSAGES = PROPERTIES_CONFIGURATION.getList(String.class, "ERROR_MESSAGES");
    public static final int SUBSCRIBE_CALLBACK_ID = PROPERTIES_CONFIGURATION.getInt("SUBSCRIBE_CALLBACK_ID");
    public static final int BLOCK_APPLIED_CALLBACK_ID = PROPERTIES_CONFIGURATION.getInt("BLOCK_APPLIED_CALLBACK_ID");
    public static final int PENDING_TRANSACTION_CALLBACK_ID = PROPERTIES_CONFIGURATION.getInt(
            "PENDING_TRANSACTION_CALLBACK_ID");
    public static final boolean NOTIFY_REMOVE_CREATE = PROPERTIES_CONFIGURATION.getBoolean("NOTIFY_REMOVE_CREATE");
    public static final boolean ENABLE_AUTO_SUBSCRIPTION = PROPERTIES_CONFIGURATION.getBoolean(
            "ENABLE_AUTO_SUBSCRIPTION");
    public static final boolean INSTANT_BROADCAST_AFTER_SIGNING = PROPERTIES_CONFIGURATION.getBoolean(
            "INSTANT_BROADCAST_AFTER_SIGNING");
    public static final boolean SIGN_LOCAL = PROPERTIES_CONFIGURATION.getBoolean("SIGN_LOCAL");
    public static final boolean OBTAIN_TX_ID_LOCAL = PROPERTIES_CONFIGURATION.getBoolean("OBTAIN_TX_ID_LOCAL");
    public static final long TIMEOUT_LISTENER = PROPERTIES_CONFIGURATION.getLong("TIMEOUT_LISTENER");
    public static final TimeUnit TIMEOUT_LISTENER_TIME_UNIT = PROPERTIES_CONFIGURATION.get(TimeUnit.class,
            "TIMEOUT_LISTENER_TIME_UNIT");
    public static final boolean SET_FEES_BEFORE_BROADCAST = PROPERTIES_CONFIGURATION.getBoolean(
            "SET_FEES_BEFORE_BROADCAST");
    public static final boolean OBTAIN_TX_ID = PROPERTIES_CONFIGURATION.getBoolean("OBTAIN_TX_ID");
    public static final int WEBSOCKET_TIMEOUT = PROPERTIES_CONFIGURATION.getInt("WEBSOCKET_TIMEOUT");
    public static final boolean RECEIVE_READ_REQUEST = PROPERTIES_CONFIGURATION.getBoolean("RECEIVE_READ_REQUEST");
    public static final boolean CLOSE_SOCKET_AFTER_WRITE = PROPERTIES_CONFIGURATION.getBoolean(
            "CLOSE_SOCKET_AFTER_WRITE");
    public static final boolean CLOSE_SOCKET_AFTER_READ = PROPERTIES_CONFIGURATION.getBoolean(
            "CLOSE_SOCKET_AFTER_READ");
    public static final String DEFAULT_WALLET_PASSWORD = PROPERTIES_CONFIGURATION.getString("DEFAULT_WALLET_PASSWORD");
    public static final boolean SEND_TRANSACTION_BY_TRANSACTION = PROPERTIES_CONFIGURATION.getBoolean(
            "SEND_TRANSACTION_BY_TRANSACTION");
    public static final UpdateMeasureTimeType UPDATE_MEASURE_TIME =
            PROPERTIES_CONFIGURATION.get(UpdateMeasureTimeType.class, "UPDATE_MEASURE_TIME");
    public static final double LISTENER_THRESHOLD = PROPERTIES_CONFIGURATION.getDouble("LISTENER_THRESHOLD");
    public static final boolean SEND_WRITE_REQUESTS = PROPERTIES_CONFIGURATION.getBoolean("SEND_WRITE_REQUESTS");
    public static final boolean SEND_READ_REQUESTS = PROPERTIES_CONFIGURATION.getBoolean("SEND_READ_REQUESTS");
    public static final boolean ENABLE_RATE_LIMITER_FOR_WRITE_PAYLOADS = PROPERTIES_CONFIGURATION.getBoolean(
            "ENABLE_RATE_LIMITER_FOR_WRITE_PAYLOADS");
    public static final List<Double> WRITE_PAYLOADS_PER_SECOND = PROPERTIES_CONFIGURATION.getList(Double.class,
            "WRITE_PAYLOADS_PER_SECOND");
    public static final boolean PREPARE_READ_PAYLOADS = PROPERTIES_CONFIGURATION.getBoolean("PREPARE_READ_PAYLOADS");
    public static final ReadPayloadType READ_PAYLOAD_TYPE = PROPERTIES_CONFIGURATION.get(ReadPayloadType.class,
            "READ_PAYLOAD_TYPE");
    public static final boolean ENABLE_RATE_LIMITER_FOR_READ_PAYLOADS = PROPERTIES_CONFIGURATION.getBoolean(
            "ENABLE_RATE_LIMITER_FOR_READ_PAYLOADS");
    public static final List<Double> READ_PAYLOADS_PER_SECOND = PROPERTIES_CONFIGURATION.getList(Double.class,
            "READ_PAYLOADS_PER_SECOND");
    public static final boolean RETURN_ON_EVENT_DUPLICATE = PROPERTIES_CONFIGURATION.getBoolean(
            "RETURN_ON_EVENT_DUPLICATE");
    public static final double LISTENER_TOTAL_THRESHOLD = PROPERTIES_CONFIGURATION.getDouble(
            "LISTENER_TOTAL_THRESHOLD");
    public static final String START_ACCOUNT_ID_PREFIX = PROPERTIES_CONFIGURATION.getString("START_ACCOUNT_ID_PREFIX");
    public static final int START_ACCOUNT_ID = PROPERTIES_CONFIGURATION.getInt("START_ACCOUNT_ID");
    public static final long WEBSOCKET_RECONNECTION_SLEEP_TIME = PROPERTIES_CONFIGURATION.getLong(
            "WEBSOCKET_RECONNECTION_SLEEP_TIME");
    public static final int RESEND_TIMES_UPON_ERROR_READ = PROPERTIES_CONFIGURATION.getInt(
            "RESEND_TIMES_UPON_ERROR_READ");
    public static final boolean USE_CUSTOM_WEBSOCKET_EXECUTOR = PROPERTIES_CONFIGURATION.getBoolean(
            "USE_CUSTOM_WEBSOCKET_EXECUTOR");
    public static final Asset FEE_ASSET_ID = new Asset(PROPERTIES_CONFIGURATION.getString("FEE_ASSET_ID"));
    public static final boolean SEND_WRITE_SYNC = PROPERTIES_CONFIGURATION.getBoolean("SEND_WRITE_SYNC");
    public static final boolean SEND_WRITE_ASYNC = PROPERTIES_CONFIGURATION.getBoolean("SEND_WRITE_ASYNC");
    public static final int MAX_CONNECTION_RETRIES = PROPERTIES_CONFIGURATION.getInt("MAX_CONNECTION_RETRIES");
    public static final int DEFAULT_FACTORY_WEBSOCKET_TIMEOUT = PROPERTIES_CONFIGURATION.getInt(
            "DEFAULT_FACTORY_WEBSOCKET_TIMEOUT");
    public static final File DEFAULT_KEY_FILE = new File(PROPERTIES_CONFIGURATION.getString("DEFAULT_KEY_FILE"));
    public static final boolean PREPARE_WRITE_PAYLOADS = PROPERTIES_CONFIGURATION.getBoolean("PREPARE_WRITE_PAYLOADS");
    public static final boolean ENABLE_LISTENER = PROPERTIES_CONFIGURATION.getBoolean("ENABLE_LISTENER");
    public static final long TIMEOUT_PREPARE_ACCOUNTS = PROPERTIES_CONFIGURATION.getLong("TIMEOUT_PREPARE_ACCOUNTS");
    public static final TimeUnit TIME_UNIT_PREPARE_ACCOUNTS = PROPERTIES_CONFIGURATION.get(TimeUnit.class,
            "TIME_UNIT_PREPARE_ACCOUNTS");
    public static final int RESEND_TIMES_UPON_ERROR_WRITE = PROPERTIES_CONFIGURATION.getInt(
            "RESEND_TIMES_UPON_ERROR_WRITE");
    public static final boolean LISTENER_AS_THREAD = PROPERTIES_CONFIGURATION.getBoolean("LISTENER_AS_THREAD");
    public static final boolean DISCONNECT_WEBSOCKETS = PROPERTIES_CONFIGURATION.getBoolean("DISCONNECT_WEBSOCKETS");
    public static final List<String> SERVER_LIST = PROPERTIES_CONFIGURATION.getList(String.class, "SERVER_LIST");
    public static final List<String> SERVER_LIST_WALLET = PROPERTIES_CONFIGURATION.getList(String.class,
            "SERVER_LIST_WALLET");
    public static final List<String> EVENT_EXISTS_SUFFIX_LIST = PROPERTIES_CONFIGURATION.getList(String.class,
            "EVENT_EXISTS_SUFFIX_LIST");
    public static final boolean HANDLE_EVENT_SYNCHRONIZED = PROPERTIES_CONFIGURATION.getBoolean(
            "HANDLE_EVENT_SYNCHRONIZED");
    private static final int CORE_POOL_SIZE = PROPERTIES_CONFIGURATION.getInt("CORE_POOL_SIZE");
    private static final int MAXIMUM_POOL_SIZE = PROPERTIES_CONFIGURATION.getInt("MAXIMUM_POOL_SIZE");
    private static final long KEEP_ALIVE_TIME = PROPERTIES_CONFIGURATION.getLong("KEEP_ALIVE_TIME");
    private static final TimeUnit TIME_UNIT = PROPERTIES_CONFIGURATION.get(TimeUnit.class, "TIME_UNIT");
    public static final boolean ENABLE_BLOCK_STATISTICS = PROPERTIES_CONFIGURATION.getBoolean("ENABLE_BLOCK_STATISTICS");
    private static BlockingQueue<Runnable> blockingQueue;

    public static final long TIMEOUT_TRANSACTION = PROPERTIES_CONFIGURATION.getInt("TIMEOUT_TRANSACTION");
    public static final TimeUnit TIMEOUT_UNIT_TRANSACTION = PROPERTIES_CONFIGURATION.get(TimeUnit.class, "TIMEOUT_UNIT_TRANSACTION");
    public static final boolean DROP_ON_TIMEOUT = PROPERTIES_CONFIGURATION.getBoolean("DROP_ON_TIMEOUT");
    public static final boolean DROP_ON_BAD_ALLOC = PROPERTIES_CONFIGURATION.getBoolean("DROP_ON_BAD_ALLOC");
    public static final boolean DROP_ON_UNIQUE_CONSTRAINT = PROPERTIES_CONFIGURATION.getBoolean("DROP_ON_UNIQUE_CONSTRAINT");

    public static Class<? extends IGraphenePayloads> WRITE_PAYLOAD_PATTERN;
    public static Class<? extends IGraphenePayloads> READ_PAYLOAD_PATTERN;

    public static final int SORT_ARRAY_LENGTH = PROPERTIES_CONFIGURATION.getInt("SORT_ARRAY_LENGTH");

    public static final int KEY_VALUE_STRING_LENGTH = PROPERTIES_CONFIGURATION.getInt("KEY_VALUE_STRING_LENGTH");

    public static final int LEN_OUTER_LOOP_MEMORY = PROPERTIES_CONFIGURATION.getInt("LEN_OUTER_LOOP_MEMORY");
    public static final int LEN_INNER_LOOP_MEMORY = PROPERTIES_CONFIGURATION.getInt("LEN_INNER_LOOP_MEMORY");
    public static final int FIRST_CHAR_INT_MEMORY = PROPERTIES_CONFIGURATION.getInt("FIRST_CHAR_INT_MEMORY");
    public static final int LENGTH_MEMORY = PROPERTIES_CONFIGURATION.getInt("LENGTH_MEMORY");
    public static final int USE_VECTOR_MEMORY_BOOL = PROPERTIES_CONFIGURATION.getInt("USE_VECTOR_MEMORY_BOOL");

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
            ABSTRACT_QUEUE = (Class<? extends AbstractQueue>) Class.forName(PROPERTIES_CONFIGURATION.getString(
                    "ABSTRACT_QUEUE"));
        } catch (ClassNotFoundException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    static {
        try {
            blockingQueue = (BlockingQueue<Runnable>) ABSTRACT_QUEUE.getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    static {
        try {
            WRITE_PAYLOAD_PATTERN = (Class<? extends IGraphenePayloads>) Class.forName(PROPERTIES_CONFIGURATION.getString("WRITE_PAYLOAD_PATTERN"));
        } catch (ClassNotFoundException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    static {
        try {
            READ_PAYLOAD_PATTERN = (Class<? extends IGraphenePayloads>) Class.forName(PROPERTIES_CONFIGURATION.getString("READ_PAYLOAD_PATTERN"));
        } catch (ClassNotFoundException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    public static final ExecutorService CUSTOM_WEBSOCKET_EXECUTOR =
            new ThreadPoolExecutor(CORE_POOL_SIZE, MAXIMUM_POOL_SIZE, KEEP_ALIVE_TIME, TIME_UNIT, blockingQueue,
                    ThreadPoolFactoryFactoryFacade.setThreadPoolFactoryWithName("custom-websocket-pool-%d"));

    static {
        try {
            WRITE_PAYLOAD = (Class<? extends IGrapheneWritePayload>) Class.forName(PROPERTIES_CONFIGURATION.getString(
                    "WRITE_PAYLOAD"));
        } catch (ClassNotFoundException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    static {
        try {
            READ_PAYLOAD = (Class<? extends IGrapheneReadPayload>) Class.forName(PROPERTIES_CONFIGURATION.getString(
                    "READ_PAYLOAD"));
        } catch (ClassNotFoundException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    static {
        try {
            I_OPERATION_TO_TRANSACTION_DISPATCHER =
                    (Class<? extends IOperationToTransactionDispatcher>) Class.forName(PROPERTIES_CONFIGURATION.getString(
                    "I_OPERATION_TO_TRANSACTION_DISPATCHER"));
        } catch (ClassNotFoundException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    static {

        if (PROPERTIES_CONFIGURATION.getList(String.class,
                "ACCT_ID_PRIVATE_KEY_MAP_ID") != null && PROPERTIES_CONFIGURATION.getList(String.class,
                "ACCT_ID_PRIVATE_KEY_MAP_KEY") != null) {

            List<String> acct_id_private_key_map_id = PROPERTIES_CONFIGURATION.getList(String.class,
                    "ACCT_ID_PRIVATE_KEY_MAP_ID");
            List<String> acct_id_private_key_map_key = PROPERTIES_CONFIGURATION.getList(String.class,
                    "ACCT_ID_PRIVATE_KEY_MAP_KEY");
            if (acct_id_private_key_map_id.size() != acct_id_private_key_map_key.size()) {
                throw new UnsupportedOperationException("Unsupported operation detected");
            }

            for (int i = 0; i < acct_id_private_key_map_id.size(); i++) {
                ACCT_ID_PRIVATE_KEY_MAP.put(acct_id_private_key_map_id.get(i), acct_id_private_key_map_key.get(i));
            }

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
            BROADCAST_TYPE = (String) RPC.class.getDeclaredField(PROPERTIES_CONFIGURATION.getString(
                    "BROADCAST_TYPE")).get(null);
        } catch (IllegalAccessException | NoSuchFieldException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    private Configuration() {
    }

}
