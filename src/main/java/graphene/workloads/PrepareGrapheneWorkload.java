package graphene.workloads;

import client.client.ClientObject;
import client.client.ClientRegistry;
import client.commoninterfaces.IPrepareWorkload;
import client.commoninterfaces.IRequestDistribution;
import client.configuration.GeneralConfiguration;
import client.statistics.IStatistics;
import client.supplements.ExceptionHandler;
import client.utils.GenericSelectionStrategy;
import co.paralleluniverse.fibers.Fiber;
import co.paralleluniverse.fibers.Suspendable;
import com.neovisionaries.ws.client.WebSocket;
import com.neovisionaries.ws.client.WebSocketException;
import com.neovisionaries.ws.client.WebSocketListener;
import graphene.configuration.Configuration;
import graphene.connection.GrapheneWebsocket;
import graphene.helper.Helper;
import graphene.listener.GrapheneAccountWebsocketListener;
import graphene.listener.GrapheneSubscription;
import graphene.listener.GrapheneWebsocketListener;
import graphene.listener.GrapheneWitnessListener;
import graphene.payload_patterns.IGraphenePayloads;
import graphene.payloads.IGrapheneReadPayload;
import graphene.payloads.IGrapheneWritePayload;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.log4j.Logger;
import org.bitcoinj.core.ECKey;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class PrepareGrapheneWorkload implements IPrepareWorkload, IRequestDistribution {

    private static final Logger LOG = Logger.getLogger(PrepareGrapheneWorkload.class);
    private static final Queue<String> PREPARED_ACCOUNT_LIST = new ConcurrentLinkedQueue<>();
    private static final int WALLET_OFFSET = 2000;
    private final List<Object> paramList = new ArrayList<>();
    private final Queue<IStatistics> iStatistics = new ConcurrentLinkedQueue<>();

    @SafeVarargs
    @Suspendable
    @Override
    public final <E> E prepareWorkload(final E... params) {

        ClientObject clientObject = (ClientObject) params[0];

        PrepareGrapheneWorkloadObject prepareGrapheneWorkloadObject = new PrepareGrapheneWorkloadObject();

        /*List<ImmutableTriple<String, String, String>> keyServerAndAccountList =
                GrapheneHelper.readKeysServersAndAccountsFromFile(Configuration.DEFAULT_KEY_FILE,
                        clientObject.getClientNumber(), clientObject.getClientNumber() + 1,
                        Configuration.START_ACCOUNT_ID_PREFIX, Configuration.START_ACCOUNT_ID);*/

        /*List<ImmutablePair<String, String>> acctIdList = GrapheneHelper.addAcctIdToPrivateKey(
                Configuration.START_ACCOUNT_ID_PREFIX,
                Configuration.START_ACCOUNT_ID + clientObject.getClientNumber(), keys);*/

        List<String> servers = PrepareGrapheneWorkloadObject.getServers();
        String serverAddress = //GenericSelectionStrategy.selectRoundRobin(servers, 1, true, false, "rr-wl", 1, false).get(0);
        GenericSelectionStrategy.selectFixed(servers, Collections.singletonList(Integer.parseInt(GeneralConfiguration.HOST_ID.split("-")[2])), true).get(0);
        //GenericSelectionStrategy.selectRoundRobin(servers, 1, true, false, "rr-wl", 1, false).get(0);
        //GenericSelectionStrategy.selectFixed(servers, Collections.singletonList(0), false).get(0);
        List<ImmutablePair<String, String>> keyAndAccountList = Helper.getIpMap().get(serverAddress);
        List<Integer> valuesToUse = IntStream.rangeClosed(Integer.parseInt(GeneralConfiguration.HOST_ID.split("-")[3]), Integer.parseInt(GeneralConfiguration.HOST_ID.split("-")[4])).boxed()
                .collect(Collectors.toList());
        List<ImmutablePair<String, String>> keyAndAccountPairList = GenericSelectionStrategy.selectFixed(keyAndAccountList, valuesToUse, true);//.get(clientObject.getClientNumber());
        /*GenericSelectionStrategy.selectRandom(keyAndAccountList,
                1, false);*/
        ImmutablePair<String, String> keyAndAccountPair =
                keyAndAccountPairList.get(clientObject.getClientNumber());
                //GenericSelectionStrategy.selectFixed(keyAndAccountPairList, Collections.singletonList(0), false).get(0);

        String privateKey = keyAndAccountPair.getLeft();
        String ecKeyForSourcePrivate = Objects.requireNonNull(privateKey); // Configuration
        // .ACCT_ID_PRIVATE_KEY_MAP.get(acctId);
        ECKey sourcePrivate = Helper.getEcKey(ecKeyForSourcePrivate);
        String acctId = keyAndAccountPair.getRight();

        URI uri = null;
        try {
            uri = new URI("ws://" + serverAddress);
        } catch (URISyntaxException ex) {
            ExceptionHandler.logException(ex);
        }

        List<String> serversWallet =
                Collections.singletonList(Objects.requireNonNull(uri).getScheme() + "://" + uri.getHost() + ":" + (uri.getPort() + WALLET_OFFSET));
                /*GenericSelectionStrategy.selectFixed(GrapheneHelper.getAccounts(true),
                Collections.singletonList(0), false);*/
        WebSocket webSocketWallet = GrapheneWebsocket.prepareWebsocket(GrapheneWebsocket.prepareWebsocketFactory(),
                serversWallet.get(0));

        List<String> serversNode = Collections.singletonList(uri.toString());
                /*GenericSelectionStrategy.selectFixed(GrapheneHelper.getAccounts(false),
                Collections.singletonList(0), false);*/
        WebSocket webSocketNode = GrapheneWebsocket.prepareWebsocket(GrapheneWebsocket.prepareWebsocketFactory(),
                serversNode.get(0));

        prepareListener(clientObject, prepareGrapheneWorkloadObject, webSocketWallet, webSocketNode);

        String password = Configuration.DEFAULT_WALLET_PASSWORD;

        LOG.info(clientObject.getClientId() + " is using (private key | server ip | acctId):" +
                keyAndAccountPair.getLeft() + " | " + serverAddress + " | " + keyAndAccountPair.getRight());

        prepareAccountList(clientObject, acctId, serversWallet, privateKey, password);

        prepareGrapheneWorkloadObject.getAcctIds().add(acctId);
        prepareGrapheneWorkloadObject.setChainId(Configuration.CHAIN_ID_BYTES);
        prepareGrapheneWorkloadObject.setSourcePrivate(sourcePrivate);

        List<ImmutableTriple<String, String, String>> keyServerAndAccountList = new ArrayList<>();
        keyServerAndAccountList.add(ImmutableTriple.of(keyAndAccountPair.getLeft(), serverAddress, keyAndAccountPair.getRight()));
        prepareGrapheneWorkloadObject.setKeyServerAndAccountList(keyServerAndAccountList);

        prepareWritePayloads(clientObject, prepareGrapheneWorkloadObject, acctId);

        prepareReadPayloads(clientObject, prepareGrapheneWorkloadObject);

        paramList.add(prepareGrapheneWorkloadObject);
        return (E) prepareGrapheneWorkloadObject;
    }

    @Suspendable
    private void prepareWritePayloads(final ClientObject clientObject,
                                      final PrepareGrapheneWorkloadObject prepareGrapheneWorkloadObject,
                                      final String acctId) {
        if (Configuration.PREPARE_WRITE_PAYLOADS) {
            List<List<IGrapheneWritePayload>> completeWritePayloadList = new ArrayList<>();
            for (int i = 0; i < GenericSelectionStrategy.selectFixed(GeneralConfiguration.CLIENT_WORKLOADS,
                    Collections.singletonList(0), false).get(0); i++) {

                IGraphenePayloads iGrapheneWritePayloadPattern = null;
                try {
                    iGrapheneWritePayloadPattern = Configuration.WRITE_PAYLOAD_PATTERN.getDeclaredConstructor().newInstance();
                } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
                    ExceptionHandler.logException(ex);
                }

                List<IGrapheneWritePayload> iGrapheneWritePayloads =
                        (List<IGrapheneWritePayload>) iGrapheneWritePayloadPattern.getPayloads(clientObject, acctId,
                                Configuration.NUMBER_OF_TRANSACTION_PAYLOADS_PER_CLIENT);
                completeWritePayloadList.add(iGrapheneWritePayloads);
            }
            prepareGrapheneWorkloadObject.setGrapheneWritePayloads(completeWritePayloadList);
        }
    }

    @Suspendable
    private void prepareReadPayloads(final ClientObject clientObject,
                                     final PrepareGrapheneWorkloadObject prepareGrapheneWorkloadObject) {
        if (Configuration.PREPARE_READ_PAYLOADS) {
            List<List<IGrapheneReadPayload>> completeReadPayloadList = new ArrayList<>();
            for (int i = 0; i < GenericSelectionStrategy.selectFixed(GeneralConfiguration.CLIENT_WORKLOADS,
                    Collections.singletonList(0), false).get(0); i++) {

                IGraphenePayloads iGrapheneReadPayloadPattern = null;
                try {
                    iGrapheneReadPayloadPattern = Configuration.READ_PAYLOAD_PATTERN.getDeclaredConstructor().newInstance();
                } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
                    ExceptionHandler.logException(ex);
                }

                List<IGrapheneReadPayload> iGrapheneReadPayloads =
                        (List<IGrapheneReadPayload>) iGrapheneReadPayloadPattern.getPayloads(clientObject);
                completeReadPayloadList.add(iGrapheneReadPayloads);
            }
            prepareGrapheneWorkloadObject.setGrapheneReadPayloads(completeReadPayloadList);
        }
    }

    @Suspendable
    private void prepareAccountList(final ClientObject clientObject,
                                    final String acctId, final List<String> serversWallet,
                                    final String privateKey, final String password) {
        synchronized (PREPARED_ACCOUNT_LIST) {
            if (!PREPARED_ACCOUNT_LIST.contains(acctId)) {

                try {
                    WebSocket webSocketWallet = getAccountWebSocket(serversWallet);
                    GrapheneAccountWebsocketListener.setExpectedResult(Arrays.asList(",\"result\":null}",
                            "The wallet must be unlocked before the password can be set\",\"data\":{}}]}}}"));
                    Helper.sendPasswordRequest(webSocketWallet, password, clientObject.getClientNumber());
                    GrapheneAccountWebsocketListener.getCompletableFuture().get(Configuration.TIMEOUT_PREPARE_ACCOUNTS, Configuration.TIME_UNIT_PREPARE_ACCOUNTS);
                    GrapheneAccountWebsocketListener.resetCompletableFuture();
                    Helper.sendUnlockCall(webSocketWallet, password, clientObject.getClientNumber());
                    GrapheneAccountWebsocketListener.getCompletableFuture().get(Configuration.TIMEOUT_PREPARE_ACCOUNTS, Configuration.TIME_UNIT_PREPARE_ACCOUNTS);
                    GrapheneAccountWebsocketListener.resetCompletableFuture();
                    GrapheneAccountWebsocketListener.setExpectedResult(Arrays.asList(",\"result\":true}",
                            ",\"result\":null}"));
                    Helper.sendImportKeyCall(webSocketWallet, acctId, privateKey,
                            clientObject.getClientNumber());
                    GrapheneAccountWebsocketListener.getCompletableFuture().get(Configuration.TIMEOUT_PREPARE_ACCOUNTS, Configuration.TIME_UNIT_PREPARE_ACCOUNTS);
                    GrapheneAccountWebsocketListener.resetCompletableFuture();
                    GrapheneAccountWebsocketListener.setExpectedResult(Arrays.asList("]}"));
                    Helper.sendImportBalanceCall(webSocketWallet, acctId, privateKey,
                            clientObject.getClientNumber());
                    GrapheneAccountWebsocketListener.getCompletableFuture().get(Configuration.TIMEOUT_PREPARE_ACCOUNTS, Configuration.TIME_UNIT_PREPARE_ACCOUNTS);
                    webSocketWallet.disconnect();

                    PREPARED_ACCOUNT_LIST.add(acctId);
                } catch (WebSocketException | InterruptedException | ExecutionException | TimeoutException ex) {
                    ExceptionHandler.logException(ex);
                }
            } else {
                LOG.info("Account already prepared: " + acctId);
            }
        }
    }

    @NotNull
    @Suspendable
    private WebSocket getAccountWebSocket(final List<String> serversWallet) throws WebSocketException {
        WebSocket webSocketWallet =
                GrapheneWebsocket.prepareWebsocket(GrapheneWebsocket.prepareWebsocketFactory(),
                        serversWallet.get(0));
        GrapheneAccountWebsocketListener grapheneAccountWebsocketListener =
                new GrapheneAccountWebsocketListener();
        webSocketWallet.addListener(grapheneAccountWebsocketListener.webSocketListener());
        webSocketWallet.connect();
        return webSocketWallet;
    }

    private static final AtomicInteger LISTENER_COUNTER = new AtomicInteger(0);

    @Suspendable
    private void prepareListener(final ClientObject clientObject,
                                 final PrepareGrapheneWorkloadObject prepareGrapheneWorkloadObject,
                                 final WebSocket webSocketWallet, final WebSocket webSocketWitness) {
        if (Configuration.ENABLE_LISTENER) {
            if (LISTENER_COUNTER.getAndIncrement() < Configuration.NUMBER_OF_LISTENERS) {
                LOG.info("Registering listener for: " + clientObject.getClientId());

                GrapheneWebsocketListener grapheneWebsocketListener = new GrapheneWebsocketListener();
                GrapheneWitnessListener grapheneWitnessListener = new GrapheneWitnessListener();

                String regex = "\"signature\":\"(.*?)\"";
                //"\"trx\":.*?\"Auxiliary_Event\":\"(.*?)\"}
                //"\"result\":\\[\\d.*?\"Auxiliary_Event\":\"(.*?)\"}";
                //"\"op\":\\[\\d.*?\"Auxiliary_Event\":\"(.*?)\"}";

                GrapheneSubscription grapheneSubscription = new GrapheneSubscription(false,
                        grapheneWitnessListener.registerWitnessResponseListener(), ClientRegistry.getClientObjects(),
                        GeneralConfiguration.CLIENT_COUNT * GenericSelectionStrategy.selectFixed(GeneralConfiguration.CLIENT_WORKLOADS,
                                Collections.singletonList(0), false).get(0) * Configuration.NUMBER_OF_TRANSACTIONS_PER_CLIENT * Configuration.NUMBER_OF_OPERATIONS_PER_TRANSACTION,
                        GeneralConfiguration.CLIENT_COUNT * GenericSelectionStrategy.selectFixed(GeneralConfiguration.CLIENT_WORKLOADS, Collections.singletonList(0), false).get(0) * Configuration.NUMBER_OF_TRANSACTIONS_PER_CLIENT * Configuration.NUMBER_OF_OPERATIONS_PER_TRANSACTION
                        , regex, Configuration.LISTENER_THRESHOLD, Configuration.LISTENER_TOTAL_THRESHOLD, iStatistics, WALLET_OFFSET);

                if (Configuration.LISTENER_AS_THREAD) {
                    Thread thread = new Thread(() -> prepareListenerLogic(prepareGrapheneWorkloadObject, webSocketWallet,
                            webSocketWitness,
                            grapheneWebsocketListener, grapheneWitnessListener, grapheneSubscription));
                    thread.setName(clientObject.getClientId() + "-listener-thread");
                    thread.setUncaughtExceptionHandler((t, ex) -> ExceptionHandler.logException(ex));
                    thread.setPriority(GeneralConfiguration.LISTENER_PRIORITY);
                    thread.start();
                } else {
                    Fiber<Void> fiber = new Fiber<>(() -> prepareListenerLogic(prepareGrapheneWorkloadObject,
                            webSocketWallet, webSocketWitness,
                            grapheneWebsocketListener, grapheneWitnessListener, grapheneSubscription));
                    fiber.setName(clientObject.getClientId() + "-listener-fiber");
                    fiber.setUncaughtExceptionHandler((t, ex) -> ExceptionHandler.logException(ex));
                    fiber.setPriority(GeneralConfiguration.LISTENER_PRIORITY);
                    fiber.start();
                }

                try {
                    grapheneSubscription.getIsSubscribed().get(Configuration.TIMEOUT_LISTENER,
                            Configuration.TIMEOUT_LISTENER_TIME_UNIT);
                } catch (InterruptedException | ExecutionException | TimeoutException ex) {
                    ExceptionHandler.logException(ex);
                }
            }
        }
    }

    @Suspendable
    private void prepareListenerLogic(final PrepareGrapheneWorkloadObject prepareGrapheneWorkloadObject,
                                      final WebSocket webSocketWallet, final WebSocket webSocketWitness,
                                      final GrapheneWebsocketListener grapheneWebsocketListener,
                                      final GrapheneWitnessListener grapheneWitnessListener,
                                      final GrapheneSubscription grapheneSubscription) {
        List<WebSocketListener> webSocketListenerList = new ArrayList<>();
        webSocketListenerList.add(grapheneWebsocketListener.webSocketListener());
        WebSocket webSocketListener = GrapheneWebsocket.connectToServer(webSocketWallet,
                webSocketListenerList);

        List<WebSocketListener> webSocketListenerListNode = new ArrayList<>();
        webSocketListenerListNode.add(grapheneSubscription);
        WebSocket webSocketWitnessListener = GrapheneWebsocket.connectToServer(webSocketWitness,
                webSocketListenerListNode);

        prepareGrapheneWorkloadObject.getWebsocketList().addAll(Arrays.asList(webSocketListener,
                webSocketWitnessListener));
        prepareGrapheneWorkloadObject.getIListenerDisconnectionLogicList().addAll(Arrays.asList(grapheneWebsocketListener,
                grapheneWitnessListener, grapheneSubscription));
    }

    @Suspendable
    public <E> List<E> getParams() {
        return (List<E>) paramList;
    }

    @SafeVarargs
    @Suspendable
    @Override
    public final <E> E endPrepareWorkload(final E... params) {
        LOG.info(((ClientObject) params[0]).getClientId() + " client preparation ended");
        return null;
    }

    @SafeVarargs
    @Override
    @Suspendable
    public final synchronized <E> Queue<IStatistics> getStatistics(final E... params) {
        return iStatistics;
    }

    @SafeVarargs
    @Override
    @Suspendable
    public final <E> void handleRequestDistribution(final E... params) {
    }
}
