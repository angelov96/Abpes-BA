package fabric.helper;

import client.supplements.ExceptionHandler;
import co.paralleluniverse.fibers.Suspendable;
import com.google.common.collect.Iterables;
import com.google.protobuf.InvalidProtocolBufferException;
import cy.agorise.graphenej.Util;
import fabric.configuration.Configuration;
import fabric.connection.CaClient;
import fabric.connection.ChannelClient;
import fabric.connection.FabricClient;
import fabric.connection.UserContext;
import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;
import org.hyperledger.fabric.protos.ledger.rwset.kvrwset.KvRwset;
import org.hyperledger.fabric.sdk.*;
import org.hyperledger.fabric.sdk.exception.InvalidArgumentException;
import org.hyperledger.fabric.sdk.exception.ProposalException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hyperledger.fabric.sdk.BlockInfo.EnvelopeType.TRANSACTION_ENVELOPE;
import static org.hyperledger.fabric.sdk.Channel.PeerOptions.createPeerOptions;

public final class Utils {

    private static final Logger LOG = Logger.getLogger(Utils.class);
    private static final List<Peer> PEER_LIST = Collections.synchronizedList(new LinkedList<>());
    private static final List<Orderer> ORDERER_LIST = Collections.synchronizedList(new LinkedList<>());

    private Utils() {
    }

    @Suspendable
    public static Channel.TransactionOptions getTransactionOptions(final UserContext userContext,
                                                                   final Collection<Orderer> orderers,
                                                                   final Channel.NOfEvents nOfEvents) {
        Channel.TransactionOptions transactionOptions = new Channel.TransactionOptions();
        transactionOptions.failFast(Configuration.ENABLE_FAIL_FAST);
        transactionOptions.shuffleOrders(Configuration.ENABLE_SHUFFLE_ORDERERS);
        transactionOptions.userContext(userContext);
        if (nOfEvents != null) {
            transactionOptions.nOfEvents(nOfEvents);
        }
        if (orderers != null) {
            transactionOptions.orderers(orderers);
        }
        return transactionOptions;
    }

    @Suspendable
    public static Channel.DiscoveryOptions getDiscoveryOptions(final String[] endpointsToIgnore,
                                                               final ServiceDiscovery.EndorsementSelector endorsementSelector,
                                                               final boolean forceDiscovery,
                                                               final boolean inspectResults,
                                                               final Channel.ServiceDiscoveryChaincodeCalls[] serviceDiscoveryChaincodeInterests) {
        Channel.DiscoveryOptions discoveryOptions = new Channel.DiscoveryOptions();
        try {
            if (endpointsToIgnore.length > 0) {
                discoveryOptions.ignoreEndpoints(endpointsToIgnore);
            }
            discoveryOptions.setEndorsementSelector(endorsementSelector);
            discoveryOptions.setForceDiscovery(forceDiscovery);
            discoveryOptions.setInspectResults(inspectResults);
            if (serviceDiscoveryChaincodeInterests.length > 0) {
                discoveryOptions.setServiceDiscoveryChaincodeInterests(serviceDiscoveryChaincodeInterests);
            }
            LOG.info("Is inspect results" + discoveryOptions.isInspectResults());
        } catch (InvalidArgumentException ex) {
            ExceptionHandler.logException(ex);
        }
        return discoveryOptions;
    }

    @Suspendable
    public static void debugPeerOptions(final Channel.PeerOptions peerOptions) {
        LOG.info("Stop events: " + peerOptions.getStopEvents());
        LOG.info("Start events: " + peerOptions.getStartEvents());
        LOG.info("Filtered block events: " + peerOptions.isRegisterEventsForFilteredBlocks());
        LOG.info("Get newest block: " + peerOptions.getNewest());
        for (final Peer.PeerRole peerRole : peerOptions.getPeerRoles()) {
            LOG.info("Peer role: " + peerRole.getPropertyName());
        }
    }

    @Suspendable
    public static void debugBlockchainInfo(final Channel channel, final Collection<Peer> peers,
                                           final User userContext) {
        try {
            BlockchainInfo blockchainInfo = channel.queryBlockchainInfo(peers, userContext);
            LOG.info("Current block hash: " + Util.bytesToHex(blockchainInfo.getCurrentBlockHash()));
            LOG.info("Previous block hash: " + Util.bytesToHex(blockchainInfo.getPreviousBlockHash()));
            LOG.info("Blockchain height: " + blockchainInfo.getHeight());
        } catch (ProposalException | InvalidArgumentException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    @Suspendable
    private static void cleanUp() {
        deleteDirectory(new File(Configuration.HLF_CERT_PATH));
    }

    @Suspendable
    private static boolean deleteDirectoryHelper(final File directory) {
        File[] contents = directory.listFiles();
        if (contents != null) {
            for (final File file : contents) {
                deleteDirectoryHelper(file);
            }
        }
        return directory.delete();
    }

    @Suspendable
    private static void deleteDirectory(final File dir) {
        boolean successfullyDeleted = true;
        if (dir.isDirectory()) {
            //FileUtils.deleteDirectory(dir);
            successfullyDeleted = deleteDirectoryHelper(dir);
        }
        if (!successfullyDeleted) {
            LOG.error("Error while deleting files");
        }
    }

    @Suspendable
    public static boolean checkIfCertificateFilesExists() {
        String fileEnding = ".ser";
        File dir = new File(Configuration.HLF_CERT_PATH);
        File adminUser =
                new File(Configuration.HLF_CERT_PATH + Configuration.ORGANIZATION + "/" + Configuration.ADMIN + fileEnding);
        File user =
                new File(Configuration.HLF_CERT_PATH + Configuration.ORGANIZATION + "/" + Configuration.USER + fileEnding);
        LOG.info("Admin user:" + adminUser + " users will be deleted, if they exist, dir exists: " + dir.exists());
        return dir.exists();
    }

    @Suspendable
    public static synchronized FabricClient createUserContext() {
        FabricClient fabricClient = null;
        if (Configuration.CLEAN_UP_CERTIFICATES && checkIfCertificateFilesExists()) {
            Utils.cleanUp();
        }
        String caUrl = Configuration.CA_URL;
        try {
            CaClient caClient = new CaClient(caUrl, new Properties());
            UserContext userContext = new UserContext();
            userContext.setName(Configuration.ADMIN);
            userContext.setAffiliation(Configuration.ORGANIZATION);
            userContext.setMspId(Configuration.ORGANIZATION_MSP);
            caClient.setAdminUserContext(userContext);
            userContext = caClient.enrollAdminUser(Configuration.ADMIN, Configuration.ADMIN_PASSWORD);

            if (Configuration.CREATE_USER) {
                userContext.setName(Configuration.USER);
                userContext.setAffiliation(Configuration.ORGANIZATION);
                userContext.setMspId(Configuration.ORGANIZATION_MSP);
                String enrollmentSecret = caClient.registerUser(Configuration.USER,
                        Configuration.AFFILIATION);
                LOG.info("Enrollment secret: " + enrollmentSecret);
                userContext = caClient.enrollUser(userContext, enrollmentSecret);
            }
            fabricClient = new FabricClient(userContext);
        } catch (Exception ex) {
            ExceptionHandler.logException(ex);
        }
        return fabricClient;
    }

    @Suspendable
    public static synchronized FabricClient createSingleUserContext(final String userId) {
        FabricClient fabricClient = null;
        String caUrl = Configuration.CA_URL;
        try {
            CaClient caClient = new CaClient(caUrl, new Properties());
            UserContext userContext = new UserContext();

            UserContext adminUserContext = Utils.readUserContext(Configuration.ADMIN,
                    Configuration.ORGANIZATION);
            caClient.setAdminUserContext(adminUserContext);

            userContext.setName(userId);
            userContext.setAffiliation(Configuration.ORGANIZATION);
            userContext.setMspId(Configuration.ORGANIZATION_MSP);
            String enrollmentSecret = caClient.registerUser(userId,
                    Configuration.AFFILIATION);
            LOG.info("Enrollment secret: " + enrollmentSecret);
            userContext = caClient.enrollUser(userContext, enrollmentSecret);
            fabricClient = new FabricClient(userContext);
        } catch (Exception ex) {
            ExceptionHandler.logException(ex);
        }
        return fabricClient;
    }

    @Suspendable
    public static UserContext readUserContext(final String username, final String affiliation) {
        String filePath = Configuration.HLF_CERT_PATH + affiliation + "/" + username + ".ser";
        File file = new File(filePath);
        UserContext userContext = null;
        if (file.exists()) {
            try (FileInputStream fileInputStream = new FileInputStream(filePath); ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream)) {
                userContext = (UserContext) objectInputStream.readObject();
            } catch (ClassNotFoundException | IOException ex) {
                ExceptionHandler.logException(ex);
            }
        }
        return userContext;
    }

    @Suspendable
    public static void writeUserContext(final UserContext userContext) {
        String directoryPath = Configuration.HLF_CERT_PATH + userContext.getAffiliation();
        String filePath = directoryPath + "/" + userContext.getName() + ".ser";
        File directory = new File(directoryPath);
        if (!directory.exists()) {
            boolean success = directory.mkdirs();
            if (!success) {
                LOG.error("Failed to create directories for user context");
            }
        }

        try (FileOutputStream fileOutputStream = new FileOutputStream(filePath);
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream)) {
            objectOutputStream.writeObject(userContext);
        } catch (IOException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    @Suspendable
    public static void getDiscoveredPeers(final Channel channel) {
        Set<Peer> expect = new HashSet<>(channel.getPeers());

        for (final Peer p : expect) {
            LOG.info(p.getName() + " <- name of discovered peer | url of discovered peer -> " + p.getUrl());
        }
    }

    @Suspendable
    public static void getDiscoveredOrderers(final Channel channel) {
        Set<Orderer> expect = new HashSet<>(channel.getOrderers());

        for (final Orderer o : expect) {
            LOG.info(o.getName() + " <- name of discovered orderer | url of discovered orderer -> " + o.getUrl());
        }
    }

    @Suspendable
    public static Channel prepareChannel(final FabricClient fabricClient, final String channelName) {
        Channel channel = null;
        try {
            ChannelClient channelClient = fabricClient.createChannelClient(channelName);
            channel = channelClient.getChannel();

            preparePeersAndOrderers();
            List<Properties> peerProperties = Configuration.PROPERTIES_PEER;
            List<Properties> ordererProperties = Configuration.PROPERTIES_ORDERER;
            Map<String, String> peers = Collections.synchronizedMap(Configuration.PEERS);
            Map<String, String> orderers = Collections.synchronizedMap(Configuration.ORDERERS);

            addPeers(fabricClient, channel, peers, peerProperties);
            addOrderers(fabricClient, channel, orderers, ordererProperties);

        } catch (InvalidArgumentException ex) {
            ExceptionHandler.logException(ex);
        }
        return channel;
    }

    @Suspendable
    private static void preparePeersAndOrderers() {
        Configuration.preparePeers();
        Configuration.preparePeersCa();
        Configuration.prepareOrderers();
        Configuration.prepareOrderersCa();
    }

    @Suspendable
    private static void addPeers(final FabricClient fabricClient, final Channel channel,
                                 final Map<String, String> peers, final List<Properties> peerProperties) {
        synchronized (PEER_LIST) {
            int peerCounter = 0;
            for (final Map.Entry<String, String> peerMap : peers.entrySet()) {
                try {
                    Peer peer = fabricClient.getInstance().newPeer(peerMap.getKey(), peerMap.getValue(),
                            peerProperties.get(peerCounter));
                    if (!PEER_LIST.contains(peer)) {
                        PEER_LIST.add(peer);
                        channel.addPeer(peer,
                                createPeerOptions().setPeerRoles(Configuration.PEER_ARRAY_SETTINGS.get((peerCounter))));
                        //peerCounter++;
                        LOG.debug("Added peer " + peer);
                    } else {
                        channel.addPeer(peer,
                                createPeerOptions().setPeerRoles(Configuration.PEER_ARRAY_SETTINGS.get((peerCounter))));
                        //peerCounter++;
                        LOG.warn("Peer " + peer + " already in list");
                    }
                } catch (InvalidArgumentException ex) {
                    ExceptionHandler.logException(ex);
                } catch (IndexOutOfBoundsException ex) {
                    LOG.error("IOOB exception, peerCounter: " + peerCounter + " number of configured peers: " + Configuration.PEER_ARRAY_SETTINGS.size());
                    LOG.error("Peer properties size: " + peerProperties.size() + " returning");
                    return;
                } finally {
                    ++peerCounter;
                }
            }
        }
    }

    @Suspendable
    private static void addOrderers(final FabricClient fabricClient, final Channel channel,
                                    final Map<String, String> orderers, final List<Properties> ordererProperties) {
        synchronized (ORDERER_LIST) {
            int ordererCounter = 0;
            for (final Map.Entry<String, String> ordererMap : orderers.entrySet()) {
                try {
                    Orderer orderer = fabricClient.getInstance().newOrderer(ordererMap.getKey(), ordererMap.getValue(),
                            ordererProperties.get(ordererCounter));
                    if (!ORDERER_LIST.contains(orderer)) {
                        ORDERER_LIST.add(orderer);
                        channel.addOrderer(orderer);
                        ordererCounter++;
                        LOG.debug("Added orderer " + orderer);
                    } else {
                        channel.addOrderer(orderer);
                        ordererCounter++;
                        LOG.warn("Orderer " + orderer + " already in list");
                    }
                } catch (InvalidArgumentException ex) {
                    ExceptionHandler.logException(ex);
                }
            }
        }
    }

    public static List<Peer> getPeerList() {
        return PEER_LIST;
    }

    public static List<Orderer> getOrdererList() {
        return ORDERER_LIST;
    }

    @Suspendable
    public static void parseBlockByRegex(final BlockEvent blockEvent) {

        Iterable<BlockEvent.TransactionEvent> transactionEvents = blockEvent.getTransactionEvents();
        String[][] contentAndTxArr = new String[Iterables.size(transactionEvents)][2];

        if (!Configuration.SPLIT_BLOCK_BY_REGEX) {
            int p = 0;
            for (final BlockEvent.TransactionEvent transactionEvent : blockEvent.getTransactionEvents()) {
                // https://blockchain-fabric.blogspot.com/2017/04/under-construction-hyperledger-fabric.html
                // "Currently, Fabric supports only one transaction action per transaction."
                LOG.info("TransactionActionInfoCount: " + transactionEvent.getTransactionActionInfoCount() +
                        (transactionEvent.getTransactionActionInfoCount() == 1 ? " TransactionActionInfoCount = 1, " +
                                "expected" : " TransactionActionInfoCount != 1, not expected"));
                for (int i = 0; i < transactionEvent.getTransactionActionInfoCount(); i++) {
                    LOG.info("ResponseMessageBytes: " + new String(transactionEvent.getTransactionActionInfo(i).getResponseMessageBytes()));
                    LOG.info("ResponseMessage: " + transactionEvent.getTransactionActionInfo(i).getResponseMessage());
                    LOG.info("ProposalResponseMessageBytes: " + new String(transactionEvent.getTransactionActionInfo(i).getProposalResponseMessageBytes()));
                    LOG.info("ProposalResponsePayload: " + new String(transactionEvent.getTransactionActionInfo(i).getProposalResponsePayload()));
                }
                String response =
                        "".equals(new String(transactionEvent.getTransactionActionInfo(0).getResponseMessageBytes())) ?
                                new String(transactionEvent.getTransactionActionInfo(0).getProposalResponsePayload()) :
                                new String(transactionEvent.getTransactionActionInfo(0).getResponseMessageBytes());
                contentAndTxArr[p][0] = response;
                contentAndTxArr[p][1] = transactionEvent.getTransactionID();
                ++p;
            }
        }

        try {
            LOG.info("Received Block with number: " + blockEvent.getBlockNumber() + " from peer: " + blockEvent.getPeer().getName() + " from channel: " + blockEvent.getChannelId());
        } catch (InvalidProtocolBufferException ex) {
            ExceptionHandler.logException(ex);
        }

        if (blockEvent.getTransactionEvents().iterator().hasNext()) {
            BlockEvent.TransactionEvent transactionEvent = blockEvent.getTransactionEvents().iterator().next();
            BlockEvent blockEventFull = transactionEvent.getBlockEvent();

            String[] lines;
            if (Configuration.SPLIT_BLOCK_BY_REGEX) {
                lines = blockEventFull.getBlock().toString().split("[\\r\\n]+");
            } else {
                lines = new String[contentAndTxArr.length];
                for (int i = 0; i < contentAndTxArr.length; i++) {
                    lines[i] = contentAndTxArr[i][0];
                }
            }

            int i = 0;
            for (final String blockEventString : lines) {
                LOG.debug("Block event as string: " + blockEventString);
                if (blockEventString.startsWith("  data:")) {
                    String patternStr;
                    try {
                        patternStr = "v" + blockEvent.getChannelId() + "\\*@" + "[a-z0-9]{64}" + ":";
                        Pattern pattern = Pattern.compile(patternStr);
                        Matcher matcher = pattern.matcher(blockEventString);
                        String txId = "";
                        if (Configuration.SPLIT_BLOCK_BY_REGEX) {
                            if (matcher.find()) {
                                String group = matcher.group();
                                txId = group.substring(group.indexOf("*@") + "*@".length(), group.lastIndexOf(':'));
                                LOG.info("txId found: " + txId);
                            } else {
                                LOG.error(txId + " txId not found");
                            }
                        } else {
                            txId = contentAndTxArr[i][1];
                            LOG.info("BlockEventString: " + blockEventString + " Txid: " + txId);
                        }
                        i++;
                    } catch (InvalidProtocolBufferException ex) {
                        ExceptionHandler.logException(ex);
                    }
                }
            }
        }

    }

    @Suspendable
    public static void printBlock(final BlockEvent blockEvent, final FabricClient fabricClient) {

        long blockNumber = blockEvent.getBlockNumber();

        System.out.printf("Current block number %d has data hash: %s\n", blockNumber,
                Hex.encodeHexString(blockEvent.getDataHash()));
        System.out.printf("Current block number %d has previous hash id: %s\n", blockNumber,
                Hex.encodeHexString(blockEvent.getPreviousHash()));
        try {
            System.out.printf("Current block number %d has calculated block hash is %s\n", blockNumber,
                    Hex.encodeHexString(SDKUtils.calculateBlockHash(fabricClient.getInstance(),
                            blockNumber, blockEvent.getPreviousHash(), blockEvent.getDataHash())));
        } catch (IOException | InvalidArgumentException ex) {
            ExceptionHandler.logException(ex);
        }

        System.out.printf("Current block number %d has %d envelope count:\n", blockNumber,
                blockEvent.getEnvelopeCount());
        int i = 0;
        for (final BlockInfo.EnvelopeInfo envelopeInfo : blockEvent.getEnvelopeInfos()) {
            ++i;

            System.out.printf("  Transaction number %d has transaction id: %s\n", i,
                    envelopeInfo.getTransactionID());
            String channelId = envelopeInfo.getChannelId();

            System.out.printf("  Transaction number %d has channel id: %s\n", i, channelId);
            System.out.printf("  Transaction number %d has epoch: %d\n", i, envelopeInfo.getEpoch());
            System.out.printf("  Transaction number %d has transaction timestamp: %tB %<te, %<tY %<tT %<Tp\n", i,
                    envelopeInfo.getTimestamp());
            System.out.printf("  Transaction number %d has type id: %s\n", i, "" + envelopeInfo.getType());

            if (envelopeInfo.getType() == TRANSACTION_ENVELOPE) {
                BlockInfo.TransactionEnvelopeInfo transactionEnvelopeInfo =
                        (BlockInfo.TransactionEnvelopeInfo) envelopeInfo;

                System.out.printf("  Transaction number %d has %d actions\n", i,
                        transactionEnvelopeInfo.getTransactionActionInfoCount());
                System.out.printf("  Transaction number %d isValid %b\n", i, transactionEnvelopeInfo.isValid());
                System.out.printf("  Transaction number %d validation code %d\n", i,
                        transactionEnvelopeInfo.getValidationCode());

                int j = 0;
                for (final BlockInfo.TransactionEnvelopeInfo.TransactionActionInfo transactionActionInfo :
                        transactionEnvelopeInfo.getTransactionActionInfos()) {
                    ++j;
                    System.out.printf("   Transaction action %d has response status %d\n", j,
                            transactionActionInfo.getResponseStatus());
                    System.out.printf("   Transaction action %d has response message byte as string: %s\n", j,
                            (new String(transactionActionInfo.getResponseMessageBytes(), StandardCharsets.UTF_8)));
                    System.out.printf("   Transaction action %d has %d endorsements\n", j,
                            transactionActionInfo.getEndorsementsCount());

                    for (int n = 0; n < transactionActionInfo.getEndorsementsCount(); ++n) {
                        BlockInfo.EndorserInfo endorserInfo = transactionActionInfo.getEndorsementInfo(n);
                        System.out.printf("Endorser %d signature: %s\n", n,
                                Hex.encodeHexString(endorserInfo.getSignature()));
                        /*System.out.printf("Endorser %d endorser: %s\n", n, new String(endorserInfo.getEndorser(),
                                StandardCharsets.UTF_8));*/
                        System.out.printf("Endorser %d endorser: %s\n", n, endorserInfo.getId());
                        System.out.printf("Endorser %d endorser msp id: %s\n", n, endorserInfo.getMspid());
                    }
                    System.out.printf("   Transaction action %d has %d chaincode input arguments\n", j,
                            transactionActionInfo.getChaincodeInputArgsCount());
                    for (int z = 0; z < transactionActionInfo.getChaincodeInputArgsCount(); ++z) {
                        System.out.printf("     Transaction action %d has chaincode input argument %d is: %s\n",
                                j, z,
                                new String(transactionActionInfo.getChaincodeInputArgs(z), StandardCharsets.UTF_8));
                    }

                    System.out.printf("   Transaction action %d proposal response status: %d\n", j,
                            transactionActionInfo.getProposalResponseStatus());
                    System.out.printf("   Transaction action %d proposal response payload: %s\n", j,
                            (new String(transactionActionInfo.getProposalResponsePayload())));

                    TxReadWriteSetInfo rwsetInfo = transactionActionInfo.getTxReadWriteSet();
                    if (null != rwsetInfo) {
                        System.out.printf("   Transaction action %d has %d name space read write sets\n", j,
                                rwsetInfo.getNsRwsetCount());

                        for (final TxReadWriteSetInfo.NsRwsetInfo nsRwsetInfo : rwsetInfo.getNsRwsetInfos()) {
                            String namespace = nsRwsetInfo.getNamespace();
                            KvRwset.KVRWSet rws = null;
                            try {
                                rws = nsRwsetInfo.getRwset();
                            } catch (InvalidProtocolBufferException ex) {
                                ExceptionHandler.logException(ex);
                            }

                            int rs = -1;
                            for (final KvRwset.KVRead readList : Objects.requireNonNull(rws).getReadsList()) {
                                rs++;

                                System.out.printf("     Namespace %s read set %d key %s version [%d:%d]\n",
                                        namespace
                                        , rs, readList.getKey(),
                                        readList.getVersion().getBlockNum(), readList.getVersion().getTxNum());
                            }

                            rs = -1;
                            for (final KvRwset.KVWrite writeList : rws.getWritesList()) {
                                rs++;
                                String valAsString = (new String(writeList.getValue().toByteArray(),
                                        StandardCharsets.UTF_8));

                                System.out.printf("     Namespace %s write set %d key %s has value '%s\n' ",
                                        namespace, rs,
                                        writeList.getKey(),
                                        valAsString);
                            }
                        }
                    }
                }
            }
        }
    }
}
