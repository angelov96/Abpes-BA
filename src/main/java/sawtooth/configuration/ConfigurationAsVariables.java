package sawtooth.configuration;

import client.configuration.GeneralConfiguration;
import org.zeromq.ZMQ;
import sawtooth.connection.ConnectionEnum;
import sawtooth.connection.SocketCreationEnum;
import sawtooth.listener.UpdateMeasureTimeType;
import sawtooth.payload_patterns.*;
import sawtooth.payload_patterns.keyvalue.SawtoothUniformKeyValueSetPayload;
import sawtooth.payloads.*;
import sawtooth.sdk.protobuf.ClientEventsSubscribeRequest;
import sawtooth.sdk.protobuf.EventFilter;
import sawtooth.sdk.protobuf.EventSubscription;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ConfigurationAsVariables {

    public static final ConnectionEnum CONNECTION_TYPE_WRITE = ConnectionEnum.ZMQ;

    public static final int NUMBER_OF_BATCHES_PER_CLIENT = 10;
    public static final int NUMBER_OF_TRANSACTIONS_PER_BATCH_PER_CLIENT = 1;
    public static final int NUMBER_OF_TRANSACTION_PAYLOADS_PER_CLIENT =
            NUMBER_OF_BATCHES_PER_CLIENT * NUMBER_OF_TRANSACTIONS_PER_BATCH_PER_CLIENT;

    public static final Class<? extends ITransactionToBatchDispatcher> I_TRANSACTION_TO_BATCH_DISPATCHER =
            BatchDispatcher.class;

    public static final boolean DECODE_DATA_AS_CBOR = true;

    public static final Class<? extends ISawtoothWritePayload> WRITE_PAYLOAD = GeneralCborTpPayload.class;
    public static final Class<? extends ISawtoothReadPayload> READ_PAYLOAD = GeneralReadPayload.class;
    public static final List<String> FAMILY_NAMES = Arrays.asList("keyValueExtended"); //"keyValue";
    public static final List<String> FAMILY_VERSIONS = Arrays.asList("0.1");
    public static final String TP_PREFIX = "a05d73"; //"888c11";
    public static final boolean USE_POST_FOR_BATCH_CHECK = false;
    public static final boolean SEND_WRITE_ASYNC = false;

    public static final Class<? extends ISawtoothPayloads> WRITE_PAYLOAD_PATTERN = SawtoothUniformKeyValueSetPayload.class;
    public static final Class<? extends ISawtoothPayloads> READ_PAYLOAD_PATTERN = SawtoothSingleReadPayload.class;

    public static final long TIMEOUT_TRANSACTION = 3;
    public static final TimeUnit TIMEOUT_UNIT_TRANSACTION = TimeUnit.MINUTES;
    public static final boolean DROP_ON_TIMEOUT = true;
    public static final boolean DROP_ON_ERROR_4 = true;

    public static final int CHECK_COMMITTED_REPOLLS = 2000;
    public static final double COMMIT_BATCHES_THRESHOLD = 1.0;
    public static final long BATCH_CHECK_REPOLL_INTERVAL = 2000;
    public static final String QUEUE_FULL_RESUBMIT_INTERVAL = "5100-9100";
    public static final boolean CHECK_BATCH_STATUS = true;
    public static final boolean ENABLE_LISTENER = true;
    public static final boolean PREPARE_WRITE_PAYLOADS = false;
    public static final boolean USE_PREPARED_WRITE_WEBSOCKET = false;
    public static final boolean WEBSOCKET_LISTENER = false;
    public static final boolean ZMQ_LISTENER = true;
    public static final boolean LISTENER_AS_THREAD = true;
    public static final boolean DISPATCH_TRANSACTIONS_TO_BATCHES = true;
    public static final boolean DISCONNECT_LISTENERS = true;
    public static final int RESEND_TIMES_UPON_ERROR_WRITE = 150;
    public static final long READ_ZMQ_SLEEP_TIME = 30;
    public static final long ZMQ_READ_TIMEOUT = 10;
    public static final int MAX_CONNECTION_RETRIES = 50;
    public static final long WRITE_ZMQ_SLEEP_TIME = 500;
    public static final boolean SEND_BATCH_BY_BATCH = true;
    public static final boolean SEND_TRANSACTION_BY_TRANSACTION = false;
    public static final long LISTENER_SLEEP_TIME = 1500;
    public static final UpdateMeasureTimeType UPDATE_MEASURE_TIME = UpdateMeasureTimeType.BY_SEND;
    public static final double LISTENER_THRESHOLD = 1.0;
    public static final List<EventSubscription> EVENT_SUBSCRIPTION_LIST = new ArrayList<>();
    public static final EventSubscription STATE_DELTA_SUBSCRIPTION = EventSubscription.newBuilder().setEventType(
            "sawtooth/state-delta").addFilters(EventFilter.newBuilder().setFilterType(EventFilter.FilterType.REGEX_ALL).build()).build();
    public static final EventSubscription BLOCK_COMMIT_SUBSCRIPTION = EventSubscription.newBuilder().setEventType(
            "sawtooth/block-commit").addFilters(EventFilter.newBuilder().setFilterType(EventFilter.FilterType.REGEX_ALL).build()).build();
    public static final EventSubscription BLOCK_COMMIT_SUBSCRIPTION_TEST = EventSubscription.newBuilder().setEventType(
            "keyValue/set").addFilters(EventFilter.newBuilder().setFilterType(EventFilter.FilterType.REGEX_ALL).build()).build();
    public static final EventSubscription BLOCK_COMMIT_SUBSCRIPTION_TEST_EXIST =
            EventSubscription.newBuilder().setEventType(
                    "keyValue/set/exist").addFilters(EventFilter.newBuilder().setFilterType(EventFilter.FilterType.REGEX_ALL).build()).build();
    public static final List<String> EVENT_DECODER_LIST = Arrays.asList(BLOCK_COMMIT_SUBSCRIPTION_TEST.getEventType(),
            BLOCK_COMMIT_SUBSCRIPTION_TEST_EXIST.getEventType());
    public static final int NUMBER_OF_LISTENERS = GeneralConfiguration.CLIENT_COUNT;
    public static final ClientEventsSubscribeRequest SUBSCRIPTIONS = ClientEventsSubscribeRequest.newBuilder()
            .addSubscriptions(BLOCK_COMMIT_SUBSCRIPTION)
            .addSubscriptions(STATE_DELTA_SUBSCRIPTION)
            .addSubscriptions(BLOCK_COMMIT_SUBSCRIPTION_TEST)
            .addSubscriptions(BLOCK_COMMIT_SUBSCRIPTION_TEST_EXIST)
            .build();
    public static final String[] VALIDATORS_TO_SEND_TRANSACTIONS_TO_WEBSOCKET = new String[]{
            "http://10.28.55.239:8008"//"http://192.168.178.39:8008/"
    };
    public static final String[] VALIDATORS_TO_SUBSCRIBE_TO_WS = new String[]{
            "ws://10.28.55.239:8008/subscriptions"
            //"ws://192.168.178.39:8008/subscriptions"
    };
    public static final String[] VALIDATORS_TO_SEND_TRANSACTIONS_TO_ZMQ = new String[]{
            "tcp://10.28.55.239:4004"
            //"tcp://192.168.178.39:4004"
    };
    public static final int TIMEOUT_BATCH_STATUS = 0;
    public static final boolean ENABLE_TRACE = false;
    public static final String[] SUBSCRIPTION_JSON_STRINGS = new String[]{
            "{'action': 'subscribe','address_prefixes': [\"" + ConfigurationAsVariables.TP_PREFIX + "\"]}"
    };
    public static final String[] UNSUBSCRIPTION_JSON_STRINGS = new String[]{
            "{'action': 'unsubscribe','address_prefixes': [\"" + ConfigurationAsVariables.TP_PREFIX + "\"]}"
    };
    public static final boolean ENABLE_DEBUGGING = true;
    public static final String ZMQ_CURVE_PUBLIC_KEY = "[/?O9nQtA=IbveNmHFR]KDcMQdRS2-UtKeXWJ7}F";
    public static final String ZMQ_CURVE_PRIVATE_KEY = "U.d:i/{?XVi4fB3m21T]noOP(.a-UbunYIU>-EQk";
    public static final boolean SEND_PING_RESPONSES = true;
    // Usually 1 is sufficient
    public static final int ZMQ_IO_THREADS = 1;
    // Default: -1 OS default
    public static final int ZMQ_TCP_KEEP_ALIVE_COUNT = -1;
    // Default: -1 OS default
    public static final int ZMQ_TCP_KEEP_ALIVE = -1;
    // Default: -1 infinite
    public static final int ZMQ_RECEIVE_TIMEOUT = -1;
    public static final int ZMQ_SEND_TIMEOUT = -1;
    // Default: 0 block or timeout
    public static final int ZMQ_SOCKET_FLAG_WRITE = ZMQ.DONTWAIT;
    public static final int ZMQ_SOCKET_FLAG_READ = ZMQ.DONTWAIT;
    public static final int ZMQ_RECEIVE_BUFFER_SIZE = -1;
    public static final int ZMQ_SEND_BUFFER_SIZE = -1;
    public static final boolean RECEIVE_READ_REQUEST = true;

    public static final long TIMEOUT_LISTENER = 1;
    public static final TimeUnit TIMEOUT_LISTENER_TIME_UNIT = TimeUnit.MINUTES;

    public static final boolean ENABLE_RATE_LIMITER_FOR_WRITE_PAYLOADS = false;
    public static final List<Double> WRITE_PAYLOADS_PER_SECOND = new ArrayList<>(Arrays.asList(1.0));
    public static final List<Double> READ_PAYLOADS_PER_SECOND = new ArrayList<>(Arrays.asList(1.0));
    public static final ConnectionEnum CONNECTION_TYPE_READ = ConnectionEnum.WebSocket;
    public static final boolean USE_PREPARED_READ_WEBSOCKET = false;
    public static final boolean PREPARE_READ_PAYLOADS = true;
    public static final int READ_REQUESTS = 1;
    public static final boolean ENABLE_RATE_LIMITER_FOR_READ_PAYLOADS = false;
    public static final boolean RETURN_ON_EVENT_DUPLICATE = true;
    public static final boolean DECODE_DATA_AS_CBOR_UPDATE_START_TIME = true;
    public static final boolean DECODE_DATA_AS_CBOR_LISTENER = true;
    public static final boolean DECODE_DATA_AS_CBOR_READ = true;
    public static final double LISTENER_TOTAL_THRESHOLD = 1.0;
    public static final long WAIT_TIME_AFTER_CHECK_WRITE_REQUEST = 5000;
    public static final int WRITE_POLL_RETRIES = 10;
    public static final boolean RETRY_WRITE_POLL_ON_FAIL = true;
    public static final boolean SEND_READ_REQUESTS = false;
    public static final boolean SEND_WRITE_REQUESTS = true;
    public static final long WEBSOCKET_RECONNECTION_SLEEP_TIME = 1000;
    public static final int RESEND_TIMES_UPON_ERROR_READ = 3;
    public static final SocketCreationEnum SOCKET_CREATION_ENUM = SocketCreationEnum.BY_WORKLOAD;
    public static final List<String> EVENT_EXISTS_SUFFIX_LIST = new ArrayList<>(Arrays.asList("/exist"));
    public static final boolean HANDLE_EVENT_SYNCHRONIZED = true;

    public static final boolean ENABLE_BLOCK_STATISTICS=true;
    public static final boolean USE_ZMQ_FOR_BLOCK_STATISTICS=true;

    public static TpEnum TP_ENUM = TpEnum.GeneralTpPayload;

    static {
        prepareEventSubscriptionListZmq();
    }

    private static void prepareEventSubscriptionListZmq() {
        EVENT_SUBSCRIPTION_LIST.add(BLOCK_COMMIT_SUBSCRIPTION);
        EVENT_SUBSCRIPTION_LIST.add(STATE_DELTA_SUBSCRIPTION);
        EVENT_SUBSCRIPTION_LIST.add(BLOCK_COMMIT_SUBSCRIPTION_TEST);
        EVENT_SUBSCRIPTION_LIST.add(BLOCK_COMMIT_SUBSCRIPTION_TEST_EXIST);
    }

}
