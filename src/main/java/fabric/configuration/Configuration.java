package fabric.configuration;

import client.configuration.GeneralConfiguration;
import client.supplements.ExceptionHandler;
import com.google.common.collect.Streams;
import fabric.payload_patterns.IFabricPayloads;
import fabric.payloads.IFabricReadPayload;
import fabric.payloads.IFabricWritePayload;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.hyperledger.fabric.sdk.Peer;
import org.hyperledger.fabric.sdk.ServiceDiscovery;
import org.hyperledger.fabric.sdk.TransactionRequest;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public final class Configuration {

    private static final Path CURRENT_RELATIVE_PATH = Paths.get("");
    private static final String CURRENT_ABSOLUTE_PATH = CURRENT_RELATIVE_PATH.toAbsolutePath().toString();
    public static final String FILE_NAME = "fabricConfiguration.properties";
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

    public static final List<Properties> PROPERTIES_PEER = new LinkedList<>();
    public static final List<Properties> PROPERTIES_ORDERER = new LinkedList<>();
    public static final Map<String, String> PEERS = new LinkedHashMap<>();
    public static final Map<String, String> ORDERERS = new LinkedHashMap<>();
    public static final List<EnumSet<Peer.PeerRole>> PEER_ARRAY_SETTINGS = new ArrayList<>();
    private static final String[][] PEER_ARRAY;
    private static final String[][] ORDERER_ARRAY;

    public static final int NUMBER_OF_LISTENERS =
            (PROPERTIES_CONFIGURATION.getInt("NUMBER_OF_LISTENERS") == -1 ? GeneralConfiguration.CLIENT_COUNT :
                    PROPERTIES_CONFIGURATION.getInt("NUMBER_OF_LISTENERS"));

    public static Class<? extends IFabricWritePayload> WRITE_PAYLOAD;
    public static Class<? extends IFabricReadPayload> READ_PAYLOAD;
    public static ServiceDiscovery.EndorsementSelector ENDORSEMENT_SELECTOR;
    public static Class<? extends AbstractQueue> ABSTRACT_QUEUE;
    public static final int NUMBER_OF_TRANSACTIONS_PER_CLIENT = PROPERTIES_CONFIGURATION.getInt(
            "NUMBER_OF_TRANSACTIONS_PER_CLIENT");
    public static final String HLF_CERT_PATH = PROPERTIES_CONFIGURATION.getString("HLF_CERT_PATH");
    public static final TransactionRequest.Type CHAINCODE_LANGUAGE =
            PROPERTIES_CONFIGURATION.get(TransactionRequest.Type.class, "CHAINCODE_LANGUAGE");
    public static final boolean SPLIT_BLOCK_BY_REGEX = PROPERTIES_CONFIGURATION.getBoolean("SPLIT_BLOCK_BY_REGEX");
    public static final boolean PRINT_BLOCK = PROPERTIES_CONFIGURATION.getBoolean("PRINT_BLOCK");
    public static final boolean PARSE_BLOCK = PROPERTIES_CONFIGURATION.getBoolean("PARSE_BLOCK");
    public static final boolean CHECK_PROPOSAL_CONSISTENCY_SET = PROPERTIES_CONFIGURATION.getBoolean(
            "CHECK_PROPOSAL_CONSISTENCY_SET");
    public static final boolean SEND_TO_ORDERER_DESPITE_READ = PROPERTIES_CONFIGURATION.getBoolean(
            "SEND_TO_ORDERER_DESPITE_READ");
    // true default
    public static final boolean ENABLE_FAIL_FAST = PROPERTIES_CONFIGURATION.getBoolean("ENABLE_FAIL_FAST");
    // true default
    public static final boolean ENABLE_SHUFFLE_ORDERERS = PROPERTIES_CONFIGURATION.getBoolean(
            "ENABLE_SHUFFLE_ORDERERS");
    public static final boolean SEND_WRITE_ASYNC = PROPERTIES_CONFIGURATION.getBoolean("SEND_WRITE_ASYNC");
    public static final boolean SEND_WRITE_SYNC = PROPERTIES_CONFIGURATION.getBoolean("SEND_WRITE_SYNC");
    public static final boolean ENABLE_LISTENER = PROPERTIES_CONFIGURATION.getBoolean("ENABLE_LISTENER");
    public static final int CORE_POOL_SIZE = PROPERTIES_CONFIGURATION.getInt("CORE_POOL_SIZE");
    public static final int MAXIMUM_POOL_SIZE = PROPERTIES_CONFIGURATION.getInt("MAXIMUM_POOL_SIZE");
    public static final long KEEP_ALIVE_TIME = PROPERTIES_CONFIGURATION.getLong("KEEP_ALIVE_TIME");
    public static final TimeUnit TIME_UNIT = PROPERTIES_CONFIGURATION.get(TimeUnit.class, "TIME_UNIT");
    public static final int PROPOSAL_WAIT_TIME = PROPERTIES_CONFIGURATION.getInt("PROPOSAL_WAIT_TIME");
    public static final String ADMIN = PROPERTIES_CONFIGURATION.getString("ADMIN");
    public static final String ADMIN_PASSWORD = PROPERTIES_CONFIGURATION.getString("ADMIN_PASSWORD");
    public static final String USER = PROPERTIES_CONFIGURATION.getString("USER");
    public static final boolean CREATE_USER = PROPERTIES_CONFIGURATION.getBoolean("CREATE_USER");

    public static final long TIMEOUT_TRANSACTION = PROPERTIES_CONFIGURATION.getInt("TIMEOUT_TRANSACTION");
    public static final TimeUnit TIMEOUT_UNIT_TRANSACTION = PROPERTIES_CONFIGURATION.get(TimeUnit.class, "TIMEOUT_UNIT_TRANSACTION");
    public static final boolean DROP_ON_TIMEOUT = PROPERTIES_CONFIGURATION.getBoolean("DROP_ON_TIMEOUT");

    // MVCC_READ_CONFLICT = 11; PHANTOM_READ_CONFLICT = 12;
    public static final boolean FORCE_DISCOVERY = PROPERTIES_CONFIGURATION.getBoolean("FORCE_DISCOVERY");
    public static final boolean USE_SERVICE_DISCOVERY = PROPERTIES_CONFIGURATION.getBoolean("USE_SERVICE_DISCOVERY");
    public static final String[] IGNORE_ENDPOINTS = PROPERTIES_CONFIGURATION.getStringArray("IGNORE_ENDPOINTS");
    public static final String ORGANIZATION_MSP = PROPERTIES_CONFIGURATION.getString("ORGANIZATION_MSP");
    public static final String ORGANIZATION = PROPERTIES_CONFIGURATION.getString("ORGANIZATION");
    public static final String AFFILIATION = PROPERTIES_CONFIGURATION.getString("AFFILIATION");
    public static final String CA_URL = PROPERTIES_CONFIGURATION.getString("CA_URL");
    public static final boolean CLEAN_UP_CERTIFICATES = PROPERTIES_CONFIGURATION.getBoolean("CLEAN_UP_CERTIFICATES");
    public static final boolean SEND_TO_ORDERER_DESPITE_READ_AND_GET = PROPERTIES_CONFIGURATION.getBoolean(
            "SEND_TO_ORDERER_DESPITE_READ_AND_GET");
    public static final boolean DEBUG_BLOCKCHAIN_INFO = PROPERTIES_CONFIGURATION.getBoolean("DEBUG_BLOCKCHAIN_INFO");
    public static final boolean LISTENER_AS_THREAD = PROPERTIES_CONFIGURATION.getBoolean("LISTENER_AS_THREAD");
    public static final boolean PREPARE_WRITE_PAYLOADS = PROPERTIES_CONFIGURATION.getBoolean("PREPARE_WRITE_PAYLOADS");
    public static final int RESEND_TIMES_UPON_ERROR_WRITE = PROPERTIES_CONFIGURATION.getInt(
            "RESEND_TIMES_UPON_ERROR_WRITE");
    public static final List<String> CHANNEL_LIST = PROPERTIES_CONFIGURATION.getList(String.class, "CHANNEL_LIST");
    public static final boolean UNREGISTER_LISTENERS = PROPERTIES_CONFIGURATION.getBoolean("UNREGISTER_LISTENERS");
    public static final boolean RECEIVE_READ_REQUEST = PROPERTIES_CONFIGURATION.getBoolean("RECEIVE_READ_REQUEST");
    public static final boolean ALLOW_CORE_THREAD_TIME_OUT = PROPERTIES_CONFIGURATION.getBoolean(
            "ALLOW_CORE_THREAD_TIME_OUT");
    public static final boolean SET_DAEMON = PROPERTIES_CONFIGURATION.getBoolean("SET_DAEMON");
    public static final double LISTENER_THRESHOLD = PROPERTIES_CONFIGURATION.getDouble("LISTENER_THRESHOLD");
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
    public static final double LISTENER_TOTAL_THRESHOLD = PROPERTIES_CONFIGURATION.getDouble(
            "LISTENER_TOTAL_THRESHOLD");
    public static final boolean SEND_WRITE_REQUESTS = PROPERTIES_CONFIGURATION.getBoolean("SEND_WRITE_REQUESTS");
    public static final boolean SEND_READ_REQUESTS = PROPERTIES_CONFIGURATION.getBoolean("SEND_READ_REQUESTS");
    public static final int RESEND_TIMES_UPON_ERROR_READ = PROPERTIES_CONFIGURATION.getInt(
            "RESEND_TIMES_UPON_ERROR_READ");
    public static final List<String> EVENT_EXISTS_SUFFIX_LIST = PROPERTIES_CONFIGURATION.getList(String.class,
            "EVENT_EXISTS_SUFFIX_LIST");
    public static final List<String> PEERS_TO_EXPECT_EVENTS_FROM = PROPERTIES_CONFIGURATION.getList(String.class,
            "PEERS_TO_EXPECT_EVENTS_FROM");
    public static final boolean SET_ORDERER_FROM_METADATA_IN_STATISTICS = PROPERTIES_CONFIGURATION.getBoolean(
            "SET_ORDERER_FROM_METADATA_IN_STATISTICS");
    public static final long TIMEOUT_LISTENER = PROPERTIES_CONFIGURATION.getLong("TIMEOUT_LISTENER");
    public static final TimeUnit TIMEOUT_LISTENER_TIME_UNIT = PROPERTIES_CONFIGURATION.get(TimeUnit.class,
            "TIMEOUT_LISTENER_TIME_UNIT");
    public static final String ORDERER_CA_PEM = PROPERTIES_CONFIGURATION.getString("ORDERER_CA_PEM");
    public static final boolean HANDLE_EVENT_SYNCHRONIZED = PROPERTIES_CONFIGURATION.getBoolean(
            "HANDLE_EVENT_SYNCHRONIZED");
    private static final int ORDERER_MAX_INBOUND = PROPERTIES_CONFIGURATION.getInt("ORDERER_MAX_INBOUND");
    private static final int PEER_MAX_INBOUND = PROPERTIES_CONFIGURATION.getInt("PEER_MAX_INBOUND");
    private static final long ORDERER_WAIT_TIME_MILLI_SECS = PROPERTIES_CONFIGURATION.getLong(
            "ORDERER_WAIT_TIME_MILLI_SECS");
    private static String caPem = PROPERTIES_CONFIGURATION.getString("caPem");
    public static final boolean ENABLE_BLOCK_STATISTICS = PROPERTIES_CONFIGURATION.getBoolean("ENABLE_BLOCK_STATISTICS");

    private Configuration() {
    }

    public static void preparePeersCa() {
        for (int i = 0; i < PEERS.size(); i++) {
            String type = "pemFile";
            Properties properties = createProperties(type);
            //properties.setProperty("trustServerCertificate", "true");
            //properties.setProperty("sslProvider", "openSSL");
            //properties.setProperty("hostnameOverride", "override");
            properties.setProperty("negotiationType", "TLS");
            //properties.setProperty("clientCertFile", "/home/parallels/Desktop/Parallels Shared
            //Folders/Home/Desktop/client.crt");
            //properties.setProperty("clientKeyFile", "/home/parallels/Desktop/Parallels Shared
            //Folders/Home/Desktop/client.key");
            properties.put("grpc.NettyChannelBuilderOption.maxInboundMessageSize", PEER_MAX_INBOUND);

            PROPERTIES_PEER.add(properties);
        }
    }

    private static Properties createProperties(final String type) {
        Properties properties = new Properties();
        properties.setProperty(type, caPem);
        return properties;
    }

    public static void prepareOrderersCa() {
        for (int i = 0; i < ORDERERS.size(); i++) {
            String type = "pemFile";
            Properties properties = createProperties(type);
            properties.put("ordererWaitTimeMilliSecs", ORDERER_WAIT_TIME_MILLI_SECS);
            properties.put("grpc.NettyChannelBuilderOption.maxInboundMessageSize", ORDERER_MAX_INBOUND);
            PROPERTIES_ORDERER.add(properties);
        }
    }

    public static void preparePeers() {
        for (final String[] peerArray : PEER_ARRAY) {
            PEERS.put(peerArray[0], peerArray[1]);
        }
    }

    public static void prepareOrderers() {
        for (final String[] ordererArray : ORDERER_ARRAY) {
            ORDERERS.put(ordererArray[0], ordererArray[1]);
        }
    }

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

    public static Class<? extends IFabricPayloads> WRITE_PAYLOAD_PATTERN;

    static {
        try {
            WRITE_PAYLOAD_PATTERN = (Class<? extends IFabricPayloads>) Class.forName(PROPERTIES_CONFIGURATION.getString("WRITE_PAYLOAD_PATTERN"));
        } catch (ClassNotFoundException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    public static Class<? extends IFabricPayloads> READ_PAYLOAD_PATTERN;

    static {
        try {
            READ_PAYLOAD_PATTERN = (Class<? extends IFabricPayloads>) Class.forName(PROPERTIES_CONFIGURATION.getString("READ_PAYLOAD_PATTERN"));
        } catch (ClassNotFoundException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    static {
        try {
            WRITE_PAYLOAD = (Class<? extends IFabricWritePayload>) Class.forName(PROPERTIES_CONFIGURATION.getString(
                    "WRITE_PAYLOAD"));
        } catch (ClassNotFoundException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    static {
        try {
            READ_PAYLOAD = (Class<? extends IFabricReadPayload>) Class.forName(PROPERTIES_CONFIGURATION.getString(
                    "READ_PAYLOAD"));
        } catch (ClassNotFoundException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    static {
        int i = 1;

        while (PROPERTIES_CONFIGURATION.getList(String.class, "PEER_SETTINGS-" + i) != null) {

            List<Peer.PeerRole> peerRoleList = new ArrayList<>();
            for (final String peerRole : PROPERTIES_CONFIGURATION.getList(String.class, "PEER_SETTINGS-" + i)) {
                peerRoleList.add(Peer.PeerRole.valueOf(peerRole));
            }
            PEER_ARRAY_SETTINGS.add(EnumSet.copyOf(peerRoleList));

            i++;
        }
    }

    static {
        try {
            ENDORSEMENT_SELECTOR =
                    (ServiceDiscovery.EndorsementSelector) ServiceDiscovery.class.getDeclaredField(PROPERTIES_CONFIGURATION.getString(
                            "ENDORSEMENT_SELECTOR")).get(null);
        } catch (IllegalAccessException | NoSuchFieldException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    static {
        try {
            ABSTRACT_QUEUE =
                    (Class<? extends AbstractQueue>) Class.forName(PROPERTIES_CONFIGURATION.getString(
                            "ABSTRACT_QUEUE"));
        } catch (ClassNotFoundException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    static {

        List<String> peer_array_name = PROPERTIES_CONFIGURATION.getList(String.class, "PEER_ARRAY_NAME");
        List<String> peer_array_url = PROPERTIES_CONFIGURATION.getList(String.class, "PEER_ARRAY_URL");

        PEER_ARRAY = Streams.zip(peer_array_name.stream(), peer_array_url.stream(), ImmutablePair::new)
                .collect(Collectors.toList()).stream().map(pair -> new String[]{pair.getLeft(), pair.getRight()})
                .toArray(String[][]::new);
    }

    static {

        List<String> orderer_array_name = PROPERTIES_CONFIGURATION.getList(String.class, "ORDERER_ARRAY_NAME");
        List<String> orderer_array_url = PROPERTIES_CONFIGURATION.getList(String.class, "ORDERER_ARRAY_URL");

        ORDERER_ARRAY = Streams.zip(orderer_array_name.stream(), orderer_array_url.stream(), ImmutablePair::new)
                .collect(Collectors.toList()).stream().map(pair -> new String[]{pair.getLeft(), pair.getRight()})
                .toArray(String[][]::new);
    }

    static {
        boolean potentialInconsistent = false;
        System.out.println("Checking for potential inconsistencies of the configuration...");
        System.out.println("Config potentially consistent: " + !potentialInconsistent);
    }

    static {
        GeneralConfiguration.notes = GeneralConfiguration.notes + " | " +
                (PROPERTIES_CONFIGURATION.getString("notes") == null ? "" : PROPERTIES_CONFIGURATION.getString("notes"
                ));
    }

}
