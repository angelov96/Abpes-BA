package quorum.workloads;

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
import io.reactivex.disposables.Disposable;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.web3j.crypto.Credentials;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameterName;
import org.web3j.protocol.core.Request;
import org.web3j.protocol.core.methods.request.EthFilter;
import org.web3j.protocol.core.methods.response.EthGetTransactionCount;
import org.web3j.protocol.websocket.WebSocketService;
import quorum.configuration.Configuration;
import quorum.connection.Websocket;
import quorum.helper.Helper;
import quorum.listener.Listener;
import quorum.payload_patterns.IQuorumPayloads;
import quorum.payloads.IQuorumReadPayload;
import quorum.payloads.IQuorumWritePayload;
import quorum.statistics.ListenerStatisticObject;
import quorum.write.WriteHelper;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class PrepareQuorumWorkload implements IPrepareWorkload, IRequestDistribution {

    private static final Logger LOG = Logger.getLogger(PrepareQuorumWorkload.class);

    private final List<PrepareQuorumWorkloadObject> paramList = new ArrayList<>();
    private final Queue<IStatistics> iStatistics = new ConcurrentLinkedQueue<>();
    private ListenerStatisticObject listenerStatisticObject;

    @Suspendable
    public ListenerStatisticObject getListenerStatisticObject() {
        return listenerStatisticObject;
    }

    @SafeVarargs
    @Suspendable
    @Override
    public final <E> E prepareWorkload(final E... params) {

        ClientObject clientObject = (ClientObject) params[0];

        /*List<ImmutableTriple<String, String, String>> addressOrWalletPasswordAndServerTripleList =
                Helper.readAddressesFromFile(Configuration.ADDRESS_OR_WALLET_FILE,
                        clientObject.getClientNumber(), clientObject.getClientNumber() + 1);*/

        PrepareQuorumWorkloadObject prepareQuorumWorkloadObject = new PrepareQuorumWorkloadObject();

        /*ImmutableTriple<String, String, String> addressOrWalletPasswordAndServerTriple =
                GenericSelectionStrategy.selectFixed
                        (addressOrWalletPasswordAndServerTripleList, Collections.singletonList(0), false).get(0);*/

        List<String> servers = PrepareQuorumWorkloadObject.getServers();
        String nodeAddress = GenericSelectionStrategy.selectFixed(servers, Collections.singletonList(Integer.parseInt(GeneralConfiguration.HOST_ID.split("-")[2])), true).get(0);
                //GenericSelectionStrategy.selectRoundRobin(servers, 1, true, false, "rr-wl", 1, false).get(0);
                //GenericSelectionStrategy.selectFixed(servers, Collections.singletonList(0), false).get(0);
        List<ImmutablePair<String, String>> addressAndPasswordList = Helper.getIpMap().get(nodeAddress);
        /*List<ImmutablePair<String, String>> addressAndPasswordSubList =
                addressAndPasswordList.subList(Configuration.START_ADDRESS, Configuration.END_ADDRESS);*/
        List<Integer> valuesToUse = IntStream.rangeClosed(Integer.parseInt(GeneralConfiguration.HOST_ID.split("-")[3]), Integer.parseInt(GeneralConfiguration.HOST_ID.split("-")[4])).boxed()
                .collect(Collectors.toList());
        ImmutablePair<String, String> addressAndPasswordPair = GenericSelectionStrategy.selectFixed(addressAndPasswordList, valuesToUse, true).get(clientObject.getClientNumber());
                /*GenericSelectionStrategy.selectRandom(addressAndPasswordList,
                1, true).get(0);*/
                    /*GenericSelectionStrategy.selectFixed(addressAndPasswordList,
                            Collections.singletonList(clientObject.getClientNumber()), false).get(0);*/

        String fromAddress;
        if (Helper.isValidJson(addressAndPasswordPair.getLeft())) {
            JSONObject jsonObject = new JSONObject(addressAndPasswordPair.getLeft());
            fromAddress = (String.valueOf(jsonObject.get("address")).startsWith("0x") ?
                    String.valueOf(jsonObject.get("address")) : "0x" + jsonObject.get("address"));
            LOG.info("Using address: " + fromAddress + " from wallet");
        } else {
            fromAddress = addressAndPasswordPair.getLeft();
            LOG.info("Using address: " + fromAddress);
        }
        String fromPassword = addressAndPasswordPair.getRight();

        Websocket websocketWrite = new Websocket();
        WebSocketService webSocketServiceWrite = websocketWrite.prepareWebsocket(nodeAddress);
        Web3j web3jWrite = websocketWrite.prepareWeb3j(webSocketServiceWrite);

        if (Configuration.OVERWRITE_READ_ADDRESS) {
            fromAddress = WriteHelper.createNewAccount(webSocketServiceWrite, fromPassword);
            LOG.info("Replaced initial address to use: " + addressAndPasswordPair.getLeft() + " with: " + fromAddress);
        }

        LOG.debug("Using address: " + fromAddress + " with password: " + fromPassword + " with server: " + nodeAddress);

        String contractAddress = GenericSelectionStrategy.selectFixed(Configuration.CONTRACT_ADDRESS_LIST,
                Collections.singletonList(0), false).get(0);

        Websocket websocketRead = new Websocket();
        WebSocketService webSocketServiceRead = websocketRead.prepareWebsocket(nodeAddress);
        Web3j web3jRead = websocketRead.prepareWeb3j(webSocketServiceRead);

        Websocket websocketListener = new Websocket();
        WebSocketService webSocketServiceListener = websocketListener.prepareWebsocket(nodeAddress);
        Web3j web3jListener = websocketListener.prepareWeb3j(webSocketServiceListener);
        prepareListener(clientObject, prepareQuorumWorkloadObject, web3jListener);

        checkAndPrepareSendRaw(clientObject, prepareQuorumWorkloadObject, fromPassword);

        prepareQuorumWorkloadObject.setFromAddress(String.valueOf(fromAddress));
        prepareQuorumWorkloadObject.setPassword(fromPassword);
        prepareQuorumWorkloadObject.setToAddress(contractAddress);
        prepareQuorumWorkloadObject.setWeb3jWrite(web3jWrite);
        prepareQuorumWorkloadObject.setWebSocketServiceWrite(webSocketServiceWrite);
        prepareQuorumWorkloadObject.setWeb3jRead(web3jRead);
        prepareQuorumWorkloadObject.setNodeAddress(nodeAddress);
        prepareQuorumWorkloadObject.setWebSocketServiceRead(webSocketServiceRead);
        prepareQuorumWorkloadObject.setNonce();

        prepareWritePayloads(clientObject, prepareQuorumWorkloadObject);
        prepareReadPayloads(clientObject, prepareQuorumWorkloadObject);

        Helper.unlock(webSocketServiceWrite, String.valueOf(fromAddress), fromPassword);

        debug(prepareQuorumWorkloadObject, web3jWrite);
        paramList.add(prepareQuorumWorkloadObject);
        return (E) prepareQuorumWorkloadObject;
    }

    @Suspendable
    private void debug(final PrepareQuorumWorkloadObject prepareQuorumWorkloadObject, final Web3j web3j) {

        if (Configuration.DEBUG_TRANSACTION_COUNT) {

            Helper.getNetworkStats(web3j);

            try {

                Request<?, EthGetTransactionCount> ethGetPendingTransactionCountReq =
                        web3j.ethGetTransactionCount(prepareQuorumWorkloadObject.getFromAddress(),
                                DefaultBlockParameterName.PENDING);
                EthGetTransactionCount ethGetPendingTransactionCount = ethGetPendingTransactionCountReq.send();
                if (Configuration.DEBUG_SENT_TRANSACTION) {
                    ethGetPendingTransactionCount =
                            (EthGetTransactionCount) Helper.debugSend(ethGetPendingTransactionCount,
                                    ethGetPendingTransactionCountReq,
                                    "PrepareQuorumWorkload, pending transactions", false);
                }

                Request<?, EthGetTransactionCount> ethGetEarliestTransactionCountReq =
                        web3j.ethGetTransactionCount(prepareQuorumWorkloadObject.getFromAddress(),
                                DefaultBlockParameterName.EARLIEST);
                EthGetTransactionCount ethGetEarliestTransactionCount = ethGetEarliestTransactionCountReq.send();
                if (Configuration.DEBUG_SENT_TRANSACTION) {
                    ethGetEarliestTransactionCount =
                            (EthGetTransactionCount) Helper.debugSend(ethGetEarliestTransactionCount,
                                    ethGetEarliestTransactionCountReq,
                                    "PrepareQuorumWorkload, earliest transactions", false);
                }

                Request<?, EthGetTransactionCount> ethGetLatestTransactionCountReq =
                        web3j.ethGetTransactionCount(prepareQuorumWorkloadObject.getFromAddress(),
                                DefaultBlockParameterName.LATEST);
                EthGetTransactionCount ethGetLatestTransactionCount = ethGetLatestTransactionCountReq.send();
                if (Configuration.DEBUG_SENT_TRANSACTION) {
                    ethGetLatestTransactionCount =
                            (EthGetTransactionCount) Helper.debugSend(ethGetLatestTransactionCount,
                                    ethGetLatestTransactionCountReq,
                                    "PrepareQuorumWorkload, latest transactions", false);
                }

                LOG.info("Pending transactions: " + ethGetPendingTransactionCount.getTransactionCount());
                LOG.info("Earliest transactions: " + ethGetEarliestTransactionCount.getTransactionCount());
                LOG.info("Latest transactions: " + ethGetLatestTransactionCount.getTransactionCount());
            } catch (IOException ex) {
                ExceptionHandler.logException(ex);
            }
        }
    }

    @Suspendable
    private void checkAndPrepareSendRaw(final ClientObject clientObject,
                                        final PrepareQuorumWorkloadObject prepareQuorumWorkloadObject,
                                        final String fromPassword) {
        if (Configuration.SEND_RAW) {
            Credentials credentials = Helper.getWalletCredentials(
                    fromPassword,
                    Configuration.WALLET_PATH_PREFIX + clientObject.getClientNumber() + Configuration.WALLET_ENDING);

            LOG.info(clientObject.getClientId() + " using wallet: " + Configuration.WALLET_PATH_PREFIX + clientObject.getClientNumber() + Configuration.WALLET_ENDING + " with address " + fromPassword);
            prepareQuorumWorkloadObject.setCredentials(credentials);
        }
    }

    @Suspendable
    private void prepareWritePayloads(final ClientObject clientObject,
                                      final PrepareQuorumWorkloadObject prepareQuorumWorkloadObject) {
        if (Configuration.PREPARE_WRITE_PAYLOADS) {
            List<List<IQuorumWritePayload>> completeWritePayloadList = new ArrayList<>();
            for (int i = 0; i < GenericSelectionStrategy.selectFixed(GeneralConfiguration.CLIENT_WORKLOADS,
                    Collections.singletonList(0), false).get(0); i++) {

                IQuorumPayloads iQuorumWritePayloadPattern = null;
                try {
                    iQuorumWritePayloadPattern =
                            Configuration.WRITE_PAYLOAD_PATTERN.getDeclaredConstructor().newInstance();
                } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
                    ExceptionHandler.logException(ex);
                }

                List<IQuorumWritePayload> iQuorumPayloads =
                        (List<IQuorumWritePayload>) iQuorumWritePayloadPattern.getPayloads(clientObject,
                                Configuration.NUMBER_OF_TRANSACTIONS_PER_CLIENT);
                completeWritePayloadList.add(iQuorumPayloads);
            }
            prepareQuorumWorkloadObject.setQuorumWritePayloads(completeWritePayloadList);
        }
    }

    @Suspendable
    private void prepareReadPayloads(final ClientObject clientObject,
                                     final PrepareQuorumWorkloadObject prepareQuorumWorkloadObject) {
        if (Configuration.PREPARE_READ_PAYLOADS) {
            List<List<IQuorumReadPayload>> completeReadPayloadList = new ArrayList<>();
            for (int i = 0; i < GenericSelectionStrategy.selectFixed(GeneralConfiguration.CLIENT_WORKLOADS,
                    Collections.singletonList(0), false).get(0); i++) {

                IQuorumPayloads iQuorumReadPayloadPattern = null;
                try {
                    iQuorumReadPayloadPattern =
                            Configuration.READ_PAYLOAD_PATTERN.getDeclaredConstructor().newInstance();
                } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
                    ExceptionHandler.logException(ex);
                }

                List<IQuorumReadPayload> iQuorumPayloads =
                        (List<IQuorumReadPayload>) iQuorumReadPayloadPattern.getPayloads(clientObject);
                completeReadPayloadList.add(iQuorumPayloads);
            }
            prepareQuorumWorkloadObject.setQuorumReadPayloads(completeReadPayloadList);
        }
    }

    private static final AtomicInteger LISTENER_COUNTER = new AtomicInteger(0);

    @Suspendable
    private void prepareListener(final ClientObject clientObject,
                                 final PrepareQuorumWorkloadObject prepareQuorumWorkloadObject, final Web3j web3j) {
        if (Configuration.ENABLE_LISTENER) {
            if (LISTENER_COUNTER.getAndIncrement() < Configuration.NUMBER_OF_LISTENERS) {
                LOG.info("Registering listener for: " + clientObject.getClientId());

                Queue<ClientObject> clientObjects = ClientRegistry.getClientObjects();
                listenerStatisticObject = new ListenerStatisticObject();
                Listener listener =
                        new Listener(
                                GeneralConfiguration.CLIENT_COUNT * GenericSelectionStrategy.selectFixed(GeneralConfiguration.CLIENT_WORKLOADS,
                                        Collections.singletonList(0), false).get(0) * Configuration.NUMBER_OF_TRANSACTIONS_PER_CLIENT,
                                GeneralConfiguration.CLIENT_COUNT * GenericSelectionStrategy.selectFixed(GeneralConfiguration.CLIENT_WORKLOADS, Collections.singletonList(0), false).get(0) * Configuration.NUMBER_OF_TRANSACTIONS_PER_CLIENT,
                                Configuration.LISTENER_THRESHOLD, Configuration.LISTENER_TOTAL_THRESHOLD);
                if (Configuration.LISTENER_AS_THREAD) {
                    Thread thread = new Thread(() -> prepareListenerLogic(clientObjects, prepareQuorumWorkloadObject,
                            web3j, listener));
                    thread.setName(clientObject.getClientId() + "-listener-thread");
                    thread.setUncaughtExceptionHandler((t, ex) -> ExceptionHandler.logException(ex));
                    thread.setPriority(GeneralConfiguration.LISTENER_PRIORITY);
                    thread.start();
                } else {
                    Fiber<Void> fiber = new Fiber<>(() -> prepareListenerLogic(clientObjects,
                            prepareQuorumWorkloadObject,
                            web3j, listener));
                    fiber.setName(clientObject.getClientId() + "-listener-fiber");
                    fiber.setUncaughtExceptionHandler((t, ex) -> ExceptionHandler.logException(ex));
                    fiber.setPriority(GeneralConfiguration.LISTENER_PRIORITY);
                    fiber.start();
                }

                try {
                    listener.getIsSubscribed().get(Configuration.TIMEOUT_LISTENER,
                            Configuration.TIMEOUT_LISTENER_TIME_UNIT);
                } catch (InterruptedException | ExecutionException | TimeoutException ex) {
                    ExceptionHandler.logException(ex);
                }
            }
        }
    }

    @Suspendable
    private void prepareListenerLogic(final Queue<ClientObject> clientObjects,
                                      final PrepareQuorumWorkloadObject prepareQuorumWorkloadObject,
                                      final Web3j web3j, final Listener listener) {
        Disposable eventLogListener = listener.registerEventLogListener(web3j, new EthFilter(),
                clientObjects, iStatistics);
        if (Configuration.ENABLE_BLOCK_STATISTICS) {
            Disposable blockListener = listener.registerBlockListener(web3j, clientObjects,
                    true);
            prepareQuorumWorkloadObject.getListener().add(blockListener);
        }
        //Disposable pendingTransactionListener = listener.registerPendingTransactionListener(web3j);
        //Disposable transactionListener = listener.registerTransactionListener(web3j);
        prepareQuorumWorkloadObject.getListener().addAll(Arrays.asList(eventLogListener/*,
                pendingTransactionListener, transactionListener*/));
        prepareQuorumWorkloadObject.getIListenerDisconnectionLogicList().add(listener);
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
