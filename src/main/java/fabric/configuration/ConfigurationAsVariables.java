package fabric.configuration;

import client.configuration.GeneralConfiguration;
import fabric.payload_patterns.FabricSingleReadPayload;
import fabric.payload_patterns.keyvalue.FabricUniformKeyValueSetPayload;
import fabric.payload_patterns.IFabricPayloads;
import fabric.payloads.GeneralWritePayload;
import fabric.payloads.GeneralReadPayload;
import fabric.payloads.IFabricReadPayload;
import fabric.payloads.IFabricWritePayload;
import org.hyperledger.fabric.sdk.Peer;
import org.hyperledger.fabric.sdk.ServiceDiscovery;
import org.hyperledger.fabric.sdk.TransactionRequest;

import java.util.*;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class ConfigurationAsVariables {

    public static final int NUMBER_OF_TRANSACTIONS_PER_CLIENT = 1;
    public static final Class<? extends IFabricWritePayload> WRITE_PAYLOAD = GeneralWritePayload.class;
    public static final Class<? extends IFabricReadPayload> READ_PAYLOAD = GeneralReadPayload.class;

    public static final int NUMBER_OF_LISTENERS = GeneralConfiguration.CLIENT_COUNT;

    public static final Class<? extends IFabricPayloads> WRITE_PAYLOAD_PATTERN = FabricUniformKeyValueSetPayload.class;
    public static final Class<? extends IFabricPayloads> READ_PAYLOAD_PATTERN = FabricSingleReadPayload.class;

    public static final String HLF_CERT_PATH = "./users/";

    public static final TransactionRequest.Type CHAINCODE_LANGUAGE = TransactionRequest.Type.GO_LANG;

    public static final boolean SPLIT_BLOCK_BY_REGEX = false;

    public static final boolean PRINT_BLOCK = true;
    public static final boolean PARSE_BLOCK = false;

    public static final boolean CHECK_PROPOSAL_CONSISTENCY_SET = true;
    public static final boolean SEND_TO_ORDERER_DESPITE_READ = false;

    // true default
    public static final boolean ENABLE_FAIL_FAST = true;
    // true default
    public static final boolean ENABLE_SHUFFLE_ORDERERS = true;

    public static final long TIMEOUT_TRANSACTION = 3;
    public static final TimeUnit TIMEOUT_UNIT_TRANSACTION = TimeUnit.MINUTES;
    public static final boolean DROP_ON_TIMEOUT = true;

    public static final boolean SEND_WRITE_ASYNC = false;
    public static final boolean SEND_WRITE_SYNC = true;

    public static final boolean ENABLE_LISTENER = true;

    public static final int CORE_POOL_SIZE = 1;
    public static final int MAXIMUM_POOL_SIZE = Integer.MAX_VALUE;
    public static final long KEEP_ALIVE_TIME = 60L;
    public static final TimeUnit TIME_UNIT = TimeUnit.SECONDS;

    public static final boolean WRITE_PROPOSAL_AND_TRANSACTION = true;
    public static final boolean WRITE_PROPOSAL = true;
    public static final boolean WRITE_TRANSACTION = true;

    public static final int PROPOSAL_WAIT_TIME = 180000;
    public static final String ADMIN = "admin";
    public static final String ADMIN_PASSWORD = "admin";
    public static final String USER = "user";
    public static final boolean CREATE_USER = false;
    public static final boolean FORCE_DISCOVERY = false;
    public static final boolean USE_SERVICE_DISCOVERY = true;

    public static final String[] IGNORE_ENDPOINTS = new String[]{};
    public static final String ORGANIZATION_MSP = "Org1";
    public static final String ORGANIZATION = "Organization1";
    public static final String AFFILIATION = "org1";
    public static final boolean ENABLE_BLOCK_STATISTICS = true;

    // MVCC_READ_CONFLICT = 11; PHANTOM_READ_CONFLICT = 12;

    public static final String CA_URL = "http://10.28.55.243:40000"; //"http://192.168.2.105:40005";//"http://192.168
    // .178.33:40005";

    public static final boolean CLEAN_UP_CERTIFICATES = true;
    public static final List<Properties> PROPERTIES_PEER = new LinkedList<>();
    public static final List<Properties> PROPERTIES_ORDERER = new LinkedList<>();
    public static final Map<String, String> PEERS = new LinkedHashMap<>();
    public static final Map<String, String> ORDERERS = new LinkedHashMap<>();

    public static final List<EnumSet<Peer.PeerRole>> PEER_ARRAY_SETTINGS = Arrays.asList(EnumSet.of(Peer.PeerRole.SERVICE_DISCOVERY, Peer.PeerRole.ENDORSING_PEER,
            Peer.PeerRole.EVENT_SOURCE, Peer.PeerRole.CHAINCODE_QUERY, Peer.PeerRole.LEDGER_QUERY),
            EnumSet.of(Peer.PeerRole.SERVICE_DISCOVERY, Peer.PeerRole.ENDORSING_PEER,
                    Peer.PeerRole.CHAINCODE_QUERY, Peer.PeerRole.LEDGER_QUERY),
            EnumSet.of(Peer.PeerRole.SERVICE_DISCOVERY, Peer.PeerRole.ENDORSING_PEER,
                    Peer.PeerRole.CHAINCODE_QUERY, Peer.PeerRole.LEDGER_QUERY),
            EnumSet.of(Peer.PeerRole.SERVICE_DISCOVERY, Peer.PeerRole.ENDORSING_PEER,
                    Peer.PeerRole.CHAINCODE_QUERY, Peer.PeerRole.LEDGER_QUERY));
    public static final boolean SEND_TO_ORDERER_DESPITE_READ_AND_GET = true;
    public static final boolean DEBUG_BLOCKCHAIN_INFO = true;
    public static final boolean LISTENER_AS_THREAD = false;
    public static final boolean PREPARE_WRITE_PAYLOADS = false;
    public static final int RESEND_TIMES_UPON_ERROR_WRITE = 0;
    public static final List<String> CHANNEL_LIST = Stream.of("testchannel").collect(Collectors.toList());
    public static final ServiceDiscovery.EndorsementSelector ENDORSEMENT_SELECTOR =
            ServiceDiscovery.EndorsementSelector.ENDORSEMENT_SELECTION_RANDOM;
    public static final boolean UNREGISTER_LISTENERS = true;
    public static final boolean RECEIVE_READ_REQUEST = true;
    public static final boolean ALLOW_CORE_THREAD_TIME_OUT = true;
    public static final boolean SET_DAEMON = true;
    public static final double LISTENER_THRESHOLD = 1.0;

    public static final boolean ENABLE_RATE_LIMITER_FOR_WRITE_PAYLOADS = false;
    public static final List<Double> WRITE_PAYLOADS_PER_SECOND = new ArrayList<>(Arrays.asList(1.0));
    public static final boolean PREPARE_READ_PAYLOADS = true;
    public static final boolean ENABLE_RATE_LIMITER_FOR_READ_PAYLOADS = false;
    public static final List<Double> READ_PAYLOADS_PER_SECOND = new ArrayList<>(Arrays.asList(1.0));
    public static final boolean RETURN_ON_EVENT_DUPLICATE = true;
    public static final double LISTENER_TOTAL_THRESHOLD = 1.0;
    public static final boolean SEND_WRITE_REQUESTS = true;
    public static final boolean SEND_READ_REQUESTS = false;
    public static final Class<? extends AbstractQueue> ABSTRACT_QUEUE = /*LinkedBlockingQueue
    .class*/ SynchronousQueue.class;
    ;
    public static final int RESEND_TIMES_UPON_ERROR_READ = 3;
    public static final List<String> EVENT_EXISTS_SUFFIX_LIST = new ArrayList<>(Arrays.asList("/exist"));
    public static final List<String> PEERS_TO_EXPECT_EVENTS_FROM = new ArrayList<>();
    public static final boolean SET_ORDERER_FROM_METADATA_IN_STATISTICS = false;
    public static final long TIMEOUT_LISTENER = 1;
    public static final TimeUnit TIMEOUT_LISTENER_TIME_UNIT = TimeUnit.MINUTES;
    public static final String ORDERER_CA_PEM = "./ordererCa.pem";
    public static final boolean HANDLE_EVENT_SYNCHRONIZED = true;
    private static final int ORDERER_MAX_INBOUND = 90000000;
    private static final int PEER_MAX_INBOUND = 90000000;
    private static final String[][] PEER_ARRAY = new String[][]{
            {"peer0.peerone.com", "grpcs://peer0.peerone.com:7000"},
            {"peer1.peerone.com", "grpcs://peer1.peerone.com:7001"},
            {"peer2.peerone.com", "grpcs://peer2.peerone.com:7002"},
            {"peer3.peerone.com", "grpcs://peer3.peerone.com:7003"},
    };
    private static final String[][] ORDERER_ARRAY = new String[][]{
            {"orderer0.ordererone.com", "grpcs://orderer0.ordererone.com:27000"},
            {"orderer1.ordererone.com", "grpcs://orderer1.ordererone.com:27001"},
            {"orderer2.ordererone.com", "grpcs://orderer2.ordererone.com:27002"},
            {"orderer3.ordererone.com", "grpcs://orderer3.ordererone.com:27003"},
            {"orderer4.ordererone.com", "grpcs://orderer4.ordererone.com:27004"},
    };
    private static final long ORDERER_WAIT_TIME_MILLI_SECS = 3000000000L;
    private static String caPem = "./ca.pem";

    static {
        boolean potentialInconsistent = false;
        System.out.println("Checking for potential inconsistencies of the configuration...");
        System.out.println("Config potentially consistent: " + !potentialInconsistent);
    }

    private ConfigurationAsVariables() {
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

}
