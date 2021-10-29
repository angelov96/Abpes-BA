package graphene.configuration;

import client.configuration.GeneralConfiguration;
import client.utils.ThreadPoolFactoryFactoryFacade;
import cy.agorise.graphenej.Asset;
import cy.agorise.graphenej.RPC;
import cy.agorise.graphenej.Util;
import graphene.listener.UpdateMeasureTimeType;
import graphene.payload_patterns.*;
import graphene.payload_patterns.keyvalue.GrapheneUniformKeyValueSetPayload;
import graphene.payloads.*;

import java.io.File;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public final class ConfigurationAsVariables {

    public static final String CHAIN_ID = "d509d0cb6b46626a43d901cb20cdc06d716dddea7b63df416a8b3f763f9b026f";
    public static final byte[] CHAIN_ID_BYTES = Util.hexToBytes(CHAIN_ID);

    public static final int NUMBER_OF_LISTENERS = GeneralConfiguration.CLIENT_COUNT;

    public static final boolean SET_SUBSCRIBE_CALLBACK = true;
    public static final boolean SET_PENDING_TRANSACTION_CALLBACK = true;
    public static final boolean SET_BLOCK_APPLIED_CALLBACK = true;
    public static final boolean SET_AUTO_SUBSCRIPTION = true;

    public static final boolean DISPATCH_OPERATIONS_TO_TRANSACTIONS = true;

    public static final int NUMBER_OF_OPERATIONS_PER_TRANSACTION =1;
    public static final int NUMBER_OF_TRANSACTIONS_PER_CLIENT = 1;
    public static final int NUMBER_OF_TRANSACTION_PAYLOADS_PER_CLIENT =
            NUMBER_OF_OPERATIONS_PER_TRANSACTION * NUMBER_OF_TRANSACTIONS_PER_CLIENT;

    public static final boolean USE_TRANSACTION_BUILDER = false;
    public static final boolean IS_TRANSACTION_EXECUTED_BY_WALLET = false;

    public static final Class<? extends IGrapheneWritePayload> WRITE_PAYLOAD = GeneralPayload.class;
    public static final Class<? extends IGrapheneReadPayload> READ_PAYLOAD = GeneralReadPayload.class;

    public static final boolean TRANSACTION_BUILDER_SIGN_WITH_INSTANT_BROADCAST = true;
    public static final boolean TRANSACTION_BUILDER_SIGN_BROADCAST = false;

    public static final String ADDRESS_PREFIX = "TEST";

    public static final String RPC_USERNAME_NODE = null;
    public static final String RPC_PASSWORD_NODE = null;

    public static final short PREFIX_LENGTH = 4;

    public static final List<String> ERROR_MESSAGES = new ArrayList<>(Arrays.asList("assert_exception",
            "method_not_found_exception", "invalid_arg_exception", "tx_missing_active_auth"));

    public static final String BROADCAST_TYPE = RPC.CALL_BROADCAST_TRANSACTION; //"broadcast_transaction_synchronous"
    public static final int SUBSCRIBE_CALLBACK_ID = 123;
    public static final int BLOCK_APPLIED_CALLBACK_ID = 456;
    public static final int PENDING_TRANSACTION_CALLBACK_ID = 789;

    public static final boolean NOTIFY_REMOVE_CREATE = true;
    public static final boolean ENABLE_AUTO_SUBSCRIPTION = true;
    public static final boolean INSTANT_BROADCAST_AFTER_SIGNING = true;
    public static final boolean SIGN_LOCAL = false;
    public static final boolean OBTAIN_TX_ID_LOCAL = false;

    public static final long TIMEOUT_LISTENER = 10;
    public static final TimeUnit TIMEOUT_LISTENER_TIME_UNIT = TimeUnit.MINUTES;

    public static final boolean SET_FEES_BEFORE_BROADCAST = true;
    public static final boolean OBTAIN_TX_ID = false;
    public static final int WEBSOCKET_TIMEOUT = 0;
    public static final boolean RECEIVE_READ_REQUEST = true;
    public static final boolean CLOSE_SOCKET_AFTER_WRITE = true;
    public static final boolean CLOSE_SOCKET_AFTER_READ = true;
    public static final String DEFAULT_WALLET_PASSWORD = "password";
    public static final boolean SEND_TRANSACTION_BY_TRANSACTION = false;
    public static final UpdateMeasureTimeType UPDATE_MEASURE_TIME = UpdateMeasureTimeType.BY_SEND;
    public static final double LISTENER_THRESHOLD = 1.0;

    public static final Class<? extends IGraphenePayloads> WRITE_PAYLOAD_PATTERN = GrapheneUniformKeyValueSetPayload.class;
    public static final Class<? extends IGraphenePayloads> READ_PAYLOAD_PATTERN = GrapheneSingleReadPayload.class;

    public static final long TIMEOUT_TRANSACTION = 3;
    public static final TimeUnit TIMEOUT_UNIT_TRANSACTION = TimeUnit.MINUTES;
    public static final boolean DROP_ON_TIMEOUT = true;
    public static final boolean DROP_ON_BAD_ALLOC = true;
    public static final boolean DROP_ON_UNIQUE_CONSTRAINT = true;

    public static final boolean SEND_WRITE_REQUESTS = true;
    public static final boolean SEND_READ_REQUESTS = false;

    public static final boolean ENABLE_BLOCK_STATISTICS = true;

    public static final boolean ENABLE_RATE_LIMITER_FOR_WRITE_PAYLOADS = false;
    public static final List<Double> PAYLOADS_PER_SECOND = new ArrayList<>(Arrays.asList(1.0 / 60));
    public static final boolean PREPARE_READ_PAYLOADS = true;
    public static final ReadPayloadType READ_PAYLOAD_TYPE = ReadPayloadType.JSON_RPC;
    public static final boolean ENABLE_RATE_LIMITER_FOR_READ_PAYLOADS = false;
    public static final List<Double> READ_PAYLOADS_PER_SECOND = new ArrayList<>(Arrays.asList(1.0));
    public static final boolean RETURN_ON_EVENT_DUPLICATE = true;
    public static final double LISTENER_TOTAL_THRESHOLD = 1.0;
    public static final String START_ACCOUNT_ID_PREFIX = "1.2.";
    public static final int START_ACCOUNT_ID = 6;
    public static final long WEBSOCKET_RECONNECTION_SLEEP_TIME = 1000;
    public static final int RESEND_TIMES_UPON_ERROR_READ = 3;
    public static final boolean USE_CUSTOM_WEBSOCKET_EXECUTOR = false;
    public static final Asset FEE_ASSET_ID = new Asset("1.3.0");
    public static final boolean SEND_WRITE_SYNC = true;
    public static final boolean SEND_WRITE_ASYNC = false;
    public static final int MAX_CONNECTION_RETRIES = 500;
    public static final int DEFAULT_FACTORY_WEBSOCKET_TIMEOUT = 0;
    public static final File DEFAULT_KEY_FILE = new File("E:\\abpes\\witness-file");
    public static final boolean PREPARE_WRITE_PAYLOADS = false;
    public static final boolean ENABLE_LISTENER = true;
    public static final long TIMEOUT_PREPARE_ACCOUNTS = 1;
    public static final TimeUnit TIME_UNIT_PREPARE_ACCOUNTS = TimeUnit.MINUTES;
    public static final Class<? extends IOperationToTransactionDispatcher> I_OPERATION_TO_TRANSACTION_DISPATCHER =
            TransactionDispatcher.class;
    public static final int RESEND_TIMES_UPON_ERROR_WRITE = 3;
    public static final boolean LISTENER_AS_THREAD = true;
    public static final boolean DISCONNECT_WEBSOCKETS = true;
    public static final Map<String, String> ACCT_ID_PRIVATE_KEY_MAP = new LinkedHashMap<String, String>() {{
        put("1.2.6", "5KYFu24R2JHAes3JXmmp6trEgucRi1wEZR5Q8zo5uVFwpEp2kb2");
        put("1.2.7", "5KASGN47d7ffwoNgETQtXpRxeEi95fCuSpSMVruyNsXD8pmckZs");
        put("1.2.8", "5JTcDDEdxLibztoen4KNSWo7R51Fyx5dmKsdLMKy7Aryiu9hWYY");
        put("1.2.9", "5KBxLBSAD1KBFmY1FCfSacoDWn78QvpXkzsko5i9ecirREYmhfV");
        put("1.2.10", "5JTJgUShAagjAdf4NEhBzccirGkt4wfjpafcSEvApBoq5uJkxzo");
        put("1.2.11", "5K2VFCzDVzPJEaUB3TgFkyxHZJ8V672Qs5Nq3qdacJ1MrDAwdGa");
        put("1.2.12", "5K7VeXpyS7v7YUV7yapmUXVSxiNc5jfUAu9j93Pm4fDjMxvYraL");
        put("1.2.13", "5JtAszRUBaSr9mHKYsbSMWNqj3UPn92CUWSnaWwZjngqe7wXcpC");
        put("1.2.14", "5KkDyjMeAZ2y2GTyhFtY5L4THh3aqZGSmb623zNdr1d7Ld5sA18");
        put("1.2.15", "5J47dykZkfRiDJ2r8gxFJfxJNk9R1oS68rmpqCBE1xhw7Amxvv9");
    }};
    public static final List<String> ACCOUNT_TEST_SERVERS = new ArrayList<>(
            Arrays.asList("ws://192.168.178.39:11001/rpc"));
    public static final List<String> ACCOUNT_TEST_SERVERS_WALLET = new ArrayList<>(
            Arrays.asList("ws://192.168.178.39:13123/rpc"));
    public static final List<String> EVENT_EXISTS_SUFFIX_LIST = new ArrayList<>(Arrays.asList("/exist"));
    public static final boolean HANDLE_EVENT_SYNCHRONIZED = false;
    private static final int CORE_POOL_SIZE = 1;
    private static final int MAXIMUM_POOL_SIZE = Integer.MAX_VALUE;
    private static final long KEEP_ALIVE_TIME = 60L;
    private static final TimeUnit TIME_UNIT = TimeUnit.SECONDS;
    public static final ExecutorService CUSTOM_WEBSOCKET_EXECUTOR =
            new ThreadPoolExecutor(CORE_POOL_SIZE, MAXIMUM_POOL_SIZE, KEEP_ALIVE_TIME, TIME_UNIT, /*new
                    LinkedBlockingQueue<>()
                    OR*/ new SynchronousQueue<>(), ThreadPoolFactoryFactoryFacade.setThreadPoolFactoryWithName("custom-websocket-pool-%d"));
    public static String filePrefix = "";
    public static String logFilePath = "";

    static {
        boolean potentialInconsistent = false;
        System.out.println("Checking for potential inconsistencies of the configuration...");
        System.out.println("Config potentially consistent: " + !potentialInconsistent);
    }

    private ConfigurationAsVariables() {
    }

    public static void setFilePrefix(final String filePrefix) {
        ConfigurationAsVariables.filePrefix = filePrefix;
    }

    public static void setLogFilePath(final String logFilePath) {
        ConfigurationAsVariables.logFilePath = logFilePath;
    }

}
