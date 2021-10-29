package corda.configuration;

import client.configuration.GeneralConfiguration;
import corda.listener.CloseFlowPoint;
import corda.payload_patterns.CordaSingleReadPayload;
import corda.payload_patterns.keyvalue.CordaUniformKeyValueSetPayload;
import corda.payload_patterns.ICordaPayloads;
import corda.payloads.GeneralFunctionArgsPayload;
import corda.payloads.GeneralReadPayload;
import corda.payloads.ICordaReadPayload;
import corda.payloads.ICordaWritePayload;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class ConfigurationAsVariables {
    public static final boolean USE_CUSTOM_CLIENT_CONFIGURATION = true;
    public static final int MAX_GRACEFUL_RECONNECTS = 5;

    public static final String RPC_USER = "user"; //"user1";
    public static final String RPC_PASSWORD = "password"; //"test";

    public static final int NUMBER_OF_TRANSACTIONS_PER_CLIENT = 1;
    public static final int RESEND_TIMES_UPON_ERROR_WRITE = 0;
    public static final int RESEND_TIMES_UPON_ERROR_READ = 3;
    public static final boolean SEND_TRACKED = true;
    public static final boolean PREPARE_WRITE_PAYLOADS = false;

    public static final boolean SEND_WRITE_ASYNC = false;
    public static final boolean SEND_WRITE_SYNC = true;

    public static final boolean LISTENER_AS_THREAD = false;

    public static final int DEFAULT_PAGE_NUMBER = 1;
    public static final int DEFAULT_PAGE_SIZE = 10000;

    public static final Class<? extends ICordaWritePayload> WRITE_PAYLOAD = GeneralFunctionArgsPayload.class;
    public static final Class<? extends ICordaReadPayload> READ_PAYLOAD = GeneralReadPayload.class;

    public static final List<String> NODE_LIST =
            Stream.of("10.28.55.241:32005" /*"192.168.2.105:32005"/*"192.168.178.22:10006"*/).collect(Collectors.toList());
    public static final boolean SET_TRANSACTION_ID = true;

    public static final long TIMEOUT_TRANSACTION = 3;
    public static final TimeUnit TIMEOUT_UNIT_TRANSACTION = TimeUnit.MINUTES;
    public static final boolean DROP_ON_TIMEOUT = true;

    public static final boolean RECEIVE_READ_REQUEST = true;

    public static final long RECONNECTION_SLEEP_TIME = 1000;
    public static final int MAX_CONNECTION_RETRIES = 100;

    public static final boolean ADD_TRANSACTION_NOTE = false;
    public static final boolean PRINT_DEBUG = true;
    public static final boolean USE_PREPARED_PROXY = true;
    public static final double LISTENER_THRESHOLD = 1.0;

    public static final Class<? extends ICordaPayloads> WRITE_PAYLOAD_PATTERN = CordaUniformKeyValueSetPayload.class;
    public static final Class<? extends ICordaPayloads> READ_PAYLOAD_PATTERN = CordaSingleReadPayload.class;

    public static final double LISTENER_TOTAL_THRESHOLD = 1.0;
    public static final int NUMBER_OF_LISTENERS = GeneralConfiguration.CLIENT_COUNT;
    public static final boolean ENABLE_LISTENER = true;
    public static final long TIMEOUT_LISTENER = 10;
    public static final TimeUnit TIMEOUT_LISTENER_TIME_UNIT = TimeUnit.MINUTES;
    public static final boolean WAIT_FOR_FLOW_DONE = true;
    public static final CloseFlowPoint CLOSE_FLOW_POINT = CloseFlowPoint.LISTEN;

    public static final boolean ENABLE_RATE_LIMITER_FOR_WRITE_PAYLOADS = false;
    public static final List<Double> WRITE_PAYLOADS_PER_SECOND = new ArrayList<>(Arrays.asList(1.0 / 60));
    public static final boolean PREPARE_READ_PAYLOADS = false;
    public static final boolean ENABLE_RATE_LIMITER_FOR_READ_PAYLOADS = false;
    public static final List<Double> READ_PAYLOADS_PER_SECOND = new ArrayList<>(Arrays.asList(1.0));
    public static final boolean RETURN_ON_EVENT_DUPLICATE = true;
    public static final boolean SEND_WRITE_REQUESTS = true;
    public static final boolean SEND_READ_REQUESTS = false;
    public static final int STRING_STYLE_MAX_RECURSIVE_DEPTH = 10;
    public static final List<String> EVENT_EXISTS_SUFFIX_LIST = new ArrayList<>(Arrays.asList("/exist"));
    public static final boolean HANDLE_EVENT_SYNCHRONIZED = true;

    static {
        boolean potentialInconsistent = false;
        System.out.println("Checking for potential inconsistencies of the configuration...");
        System.out.println("Config potentially consistent: " + !potentialInconsistent);
    }

    private ConfigurationAsVariables() {
    }

}
