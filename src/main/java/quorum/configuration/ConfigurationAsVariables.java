package quorum.configuration;

import client.configuration.GeneralConfiguration;
import org.web3j.abi.TypeReference;
import org.web3j.abi.datatypes.Utf8String;
import org.web3j.protocol.core.DefaultBlockParameter;
import org.web3j.protocol.core.DefaultBlockParameterName;
import quorum.payload_patterns.IQuorumPayloads;
import quorum.payload_patterns.QuorumSingleReadPayload;
import quorum.payload_patterns.keyvalue.QuorumUniformKeyValueSetPayload;
import quorum.payloads.GeneralPayload;
import quorum.payloads.GeneralReadPayload;
import quorum.payloads.IQuorumReadPayload;
import quorum.payloads.IQuorumWritePayload;

import java.io.File;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConfigurationAsVariables {

    public static final DefaultBlockParameter DEFAULT_BLOCK_PARAMETER = DefaultBlockParameterName.LATEST;
    public static final Class<? extends IQuorumWritePayload> WRITE_PAYLOAD = GeneralPayload.class;
    public static final Class<? extends IQuorumReadPayload> READ_PAYLOAD = GeneralReadPayload.class;

    public static final boolean SEND_WRITE_REQUESTS = true;
    public static final boolean SEND_READ_REQUESTS = false;

    public static final boolean DISTRIBUTED_NONCE_HANDLING = true;

    public static final long DATABASE_SLEEP_TIME = 1000;

    public static final Class<? extends IQuorumPayloads> WRITE_PAYLOAD_PATTERN = QuorumUniformKeyValueSetPayload.class;
    public static final Class<? extends IQuorumPayloads> READ_PAYLOAD_PATTERN = QuorumSingleReadPayload.class;

    public static final int NUMBER_OF_LISTENERS = GeneralConfiguration.CLIENT_COUNT;

    public static final File ADDRESS_OR_WALLET_FILE = new File("C:\\Users\\frank\\Desktop/test.txt");

    public static final List<String> CONTRACT_ADDRESS_LIST =
            Stream.of("0x01c860bbe76f913aad0a1426d9f87ac639ed5553").collect(Collectors.toList());

    public static final boolean INCLUDE_RAW_RESPONSES = false;
    public static final int NUMBER_OF_TRANSACTIONS_PER_CLIENT = 10;
    public static final BigInteger DEFAULT_GAS_PRICE = BigInteger.ZERO;
    public static final BigInteger DEFAULT_GAS_LIMIT = BigInteger.valueOf(1000000000L);
    public static final BigInteger DEFAULT_UNLOCK_DURATION = BigInteger.valueOf(1000000000L);
    public static final int LOGIN_RETRIES = 10;
    public static final int TRANSACTION_RECEIPT_RETRIES = 20;
    public static final int TRANSACTION_RECEIPT_SLEEP = 500;
    public static final boolean SEND_WRITE_ASYNC = true;
    public static final boolean SEND_WRITE_SYNC = false;
    public static final boolean SEND_READ_ASYNC = false;
    public static final boolean SEND_READ_SYNC = true;
    public static final boolean DEBUG_SENT_TRANSACTION = true;
    public static final boolean SIGN_RAW_LOCAL = false;
    public static final boolean SIGN_RAW_WEB3J = true;
    public static final boolean SEND_RAW = false;
    public static final boolean CALCULATE_TX_HASH = false;
    public static final boolean DECODE_READ_DATA = true;
    public static final boolean DIRECT_READ_AFTER_WRITE = false;
    public static final int RESEND_TIMES_UPON_ERROR_WRITE = 1;

    public static final long TIMEOUT_TRANSACTION = 3;
    public static final TimeUnit TIMEOUT_UNIT_TRANSACTION = TimeUnit.MINUTES;
    public static final boolean DROP_ON_TIMEOUT = true;

    public static final long TIMEOUT_LISTENER = 10;
    public static final TimeUnit TIMEOUT_LISTENER_TIME_UNIT = TimeUnit.MINUTES;

    public static final boolean MEASURE_BY_RECEIPT = true;

    public static final boolean LISTENER_AS_THREAD = true;

    public static final List<TypeReference<?>> DEFAULT_EVENT_TYPE_REFERENCE_LIST =
            Stream.of(TypeReference.create(Utf8String.class), TypeReference.create(Utf8String.class)).collect(Collectors.toList());

    public static final boolean NON_RAW_NULL_NONCE_HANDLING = false;
    public static final int MAX_CONNECTION_RETRIES = 50;
    public static final int DEFAULT_BLOCK_TIME = 15 * 1000;

    public static final String WALLET_PATH_PREFIX = "E:\\abpes\\wallet-";
    public static final String WALLET_ENDING = ".json";
    public static final boolean DEBUG_TRANSACTION_COUNT = false;
    public static final boolean PREPARE_WRITE_PAYLOADS = false;
    public static final boolean ENABLE_LISTENER = true;
    public static final boolean UNREGISTER_LISTENERS = true;
    public static final boolean RECEIVE_READ_REQUEST = true;
    public static final boolean USE_PREPARED_WEBSOCKET_SERVICE_WRITE = false;
    public static final boolean USE_PREPARED_WEB3J_WRITE = true;
    public static final double LISTENER_THRESHOLD = 1.0;
    public static final boolean OVERWRITE_READ_ADDRESS = true;

    public static final boolean ENABLE_RATE_LIMITER_FOR_WRITE_PAYLOADS = false;
    public static final List<Double> WRITE_PAYLOADS_PER_SECOND = new ArrayList<>(Arrays.asList(1.0));
    public static final boolean USE_PREPARED_WEB3J_READ = true;
    public static final boolean USE_PREPARED_WEBSOCKET_SERVICE_READ = false;
    public static final boolean ENABLE_RATE_LIMITER_FOR_READ_PAYLOADS = false;
    public static final List<Double> READ_PAYLOADS_PER_SECOND = new ArrayList<>(Arrays.asList(1.0));
    public static final boolean PREPARE_READ_PAYLOADS = true;
    public static final boolean RETURN_ON_EVENT_DUPLICATE = true;
    public static final double LISTENER_TOTAL_THRESHOLD = 1.0;
    public static final long WEBSOCKET_RECONNECTION_SLEEP_TIME = 1000;
    public static final int RESEND_TIMES_UPON_ERROR_READ = 3;
    public static final List<String> EVENT_EXISTS_SUFFIX_LIST = new ArrayList<>(Arrays.asList("/exist"));
    public static final boolean HANDLE_EVENT_SYNCHRONIZED = true;
    public static final int START_ADDRESS = -1;
    public static final int END_ADDRESS = -1;
    public static final boolean ENABLE_BLOCK_STATISTICS = true;

    static {
        boolean potentialInconsistent = false;
        System.out.println("Checking for potential inconsistencies of the configuration...");
        System.out.println("Config potentially consistent: " + !potentialInconsistent);
    }

}