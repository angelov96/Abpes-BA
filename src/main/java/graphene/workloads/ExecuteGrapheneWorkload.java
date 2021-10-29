package graphene.workloads;

import client.client.ClientObject;
import client.commoninterfaces.IExecuteWorkload;
import client.commoninterfaces.IListenerDisconnectionLogic;
import client.commoninterfaces.IRequestDistribution;
import client.configuration.GeneralConfiguration;
import client.statistics.IStatistics;
import client.statistics.ListenerReferenceValues;
import client.supplements.ExceptionHandler;
import client.utils.GenericSelectionStrategy;
import client.utils.NumberGenerator;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.fibers.Suspendable;
import co.paralleluniverse.strands.Strand;
import com.google.common.util.concurrent.RateLimiter;
import com.neovisionaries.ws.client.WebSocket;
import com.neovisionaries.ws.client.WebSocketException;
import graphene.components.BaseOperation;
import graphene.components.Transaction;
import graphene.configuration.Configuration;
import graphene.connection.GrapheneWebsocket;
import graphene.helper.Helper;
import graphene.listener.GrapheneSubscription;
import graphene.listener.GrapheneWitnessListener;
import graphene.listener.UpdateMeasureTimeType;
import graphene.payload_patterns.IGraphenePayloads;
import graphene.payload_patterns.IOperationToTransactionDispatcher;
import graphene.payloads.IGrapheneReadPayload;
import graphene.payloads.IGrapheneWritePayload;
import graphene.payloads.ReadPayloadType;
import graphene.read.IReadingMethod;
import graphene.read.Read;
import graphene.read.ReadByObject;
import graphene.read.ReadByObjectWallet;
import graphene.statistics.ReadStatisticObject;
import graphene.statistics.WriteStatisticObject;
import graphene.write.Write;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.log4j.Logger;
import org.hibernate.cfg.NotYetImplementedException;

import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class ExecuteGrapheneWorkload implements IExecuteWorkload, IRequestDistribution {

    private static final Logger LOG = Logger.getLogger(ExecuteGrapheneWorkload.class);
    private static final String READ_SUFFIX = "-read";
    private static final String WRITE_SUFFIX = "-write";
    private static final int WALLET_OFFSET = 2000;
    private final Queue<IStatistics> iStatistics = new ConcurrentLinkedQueue<>();
    private int writeRequests;
    private int readRequests;
    private IOperationToTransactionDispatcher transactionDispatcher;

    private static final RateLimiter rateLimiter = Configuration.ENABLE_RATE_LIMITER_FOR_WRITE_PAYLOADS ?
            RateLimiter.create(
                    GenericSelectionStrategy.selectFixed(
                            Configuration.WRITE_PAYLOADS_PER_SECOND, Collections.singletonList(0), false).get(0)
            ) : null;

    private int tn;

    @SafeVarargs
    @Override
    @Suspendable
    public final <E> E executeWorkload(final E... params) {

        ClientObject clientObject = (ClientObject) params[1];
        int workloadId = Integer.parseInt(String.valueOf(params[2])) + 1;

        List<PrepareGrapheneWorkloadObject> listOfWorkloadObjects =
                GenericSelectionStrategy.selectFixed(((ArrayList<PrepareGrapheneWorkloadObject>) params[0]),
                        java.util.Collections.singletonList(0), false);
        PrepareGrapheneWorkloadObject prepareWorkloadObject = listOfWorkloadObjects.get(0);

        if (Configuration.SEND_WRITE_REQUESTS) {

            String acctId = GenericSelectionStrategy.selectFixed(prepareWorkloadObject.getAcctIds(),
                    Collections.singletonList(0), false).get(0);

            List<IGrapheneWritePayload> iGrapheneWritePayloads = prepareWritePayloads(clientObject,
                    listOfWorkloadObjects,
                    acctId);

            long numberOfOperationsPerTransaction = Configuration.NUMBER_OF_OPERATIONS_PER_TRANSACTION;
            int transactionCounter = 0;
            List<List<BaseOperation>> operationList = new ArrayList<>();

            debugDispatcherValues(iGrapheneWritePayloads);

            prepareExpectedEventMap(clientObject, iGrapheneWritePayloads);

            dispatch(iGrapheneWritePayloads, numberOfOperationsPerTransaction, transactionCounter, operationList,
                    clientObject);

            /*RateLimiter rateLimiter = Configuration.ENABLE_RATE_LIMITER_FOR_WRITE_PAYLOADS ?
                    RateLimiter.create(
                            GenericSelectionStrategy.selectFixed(
                                    Configuration.WRITE_PAYLOADS_PER_SECOND, Collections.singletonList(0), false).get(0)
                    ) : null;*/

            for (final List<BaseOperation> baseOperations : operationList) {

                Write write = new Write();
                if (Configuration.SEND_TRANSACTION_BY_TRANSACTION) {
                    for (final BaseOperation baseOperation : baseOperations) {

                        /*if (Configuration.ENABLE_RATE_LIMITER_FOR_WRITE_PAYLOADS) {
                            rateLimiter.acquire();
                        }*/

                        Transaction transaction = new Transaction(prepareWorkloadObject.getChainId(),
                                prepareWorkloadObject.getSourcePrivate(), null,
                                Collections.singletonList(baseOperation));

                        List<String> eventList = new ArrayList<>();
                        if (Configuration.UPDATE_MEASURE_TIME == UpdateMeasureTimeType.BY_TRANSACTION) {
                            eventList = updateStartTimeByMap(transaction,
                                    clientObject);
                        }

                        //handleRequestDistribution(clientObject.getClientId());

                        WriteStatisticObject writeStatisticObject = new WriteStatisticObject();

                        writeStatisticObject.getAssocEventList().addAll(eventList);

                        writeStatisticObject.setCurrentTimeStart(System.currentTimeMillis());
                        write(transaction, write,
                                writeStatisticObject, prepareWorkloadObject, clientObject);
                        writeStatisticObject.setCurrentTimeEnd(System.currentTimeMillis());
                        writeStatisticObject.setRequestNumber(++writeRequests);
                        writeStatisticObject.setClientId(clientObject.getClientId());
                        writeStatisticObject.setRequestId("clid-" + clientObject.getClientId() + "-write-" + writeRequests + "-wlid" +
                                "-" + workloadId + "-tnid-" + ++tn);
                        updateContainedPayloadTypeByMap(transaction,
                                writeStatisticObject);
                        iStatistics.add(writeStatisticObject);
                    }
                } else {

                    /*if (Configuration.ENABLE_RATE_LIMITER_FOR_WRITE_PAYLOADS) {
                        rateLimiter.acquire();
                    }*/

                    Transaction transaction = new Transaction(prepareWorkloadObject.getChainId(),
                            prepareWorkloadObject.getSourcePrivate(), null, baseOperations);

                    List<String> eventList = new ArrayList<>();
                    if (Configuration.UPDATE_MEASURE_TIME == UpdateMeasureTimeType.BY_SEND) {
                        eventList = updateStartTimeByMap(transaction,
                                clientObject);
                    }

                    //handleRequestDistribution(clientObject.getClientId());

                    WriteStatisticObject writeStatisticObject = new WriteStatisticObject();

                    writeStatisticObject.getAssocEventList().addAll(eventList);

                    writeStatisticObject.setCurrentTimeStart(System.currentTimeMillis());
                    write(transaction, write,
                            writeStatisticObject, prepareWorkloadObject, clientObject);
                    writeStatisticObject.setCurrentTimeEnd(System.currentTimeMillis());
                    writeStatisticObject.setRequestNumber(++writeRequests);
                    writeStatisticObject.setClientId(clientObject.getClientId());
                    writeStatisticObject.setRequestId("clid-" + clientObject.getClientId() + "-write-" + writeRequests +
                            "-wlid" +
                            "-" + workloadId + "-tnid-" + ++tn);
                    updateContainedPayloadTypeByMap(transaction,
                            writeStatisticObject);
                    iStatistics.add(writeStatisticObject);
                }

            }
        }

        if (Configuration.SEND_READ_REQUESTS) {
            List<IGrapheneReadPayload> iGrapheneReadPayloads = prepareReadPayloads(clientObject, listOfWorkloadObjects);
            read(iGrapheneReadPayloads, clientObject, workloadId);
        }

        // awaitEndOfExecution(prepareWorkloadObject);

        // disconnectWebsockets(clientObject, prepareWorkloadObject);

        return null;
    }

    @Suspendable
    private List<String> updateStartTimeByMap(final Transaction transaction,
                                              final ClientObject clientObject) {
        List<String> eventList = new ArrayList<>();

        transaction.getOperations().forEach(operation -> {
            IGrapheneWritePayload payload = this.transactionDispatcher.getPayloadMapping().get(operation);

            Map<String, MutablePair<Long, Long>> valueMap =
                    GrapheneSubscription.getObtainedEventsMap().get(clientObject.getClientId());

            valueMap.get(payload.getEventPrefix() + payload.getSignature()).setLeft(System.nanoTime());

            Map<String, MutablePair<Long, Long>> stringMutablePairMap =
                    ListenerReferenceValues.getTimeMap().computeIfAbsent(clientObject.getClientId(),
                            c -> new ConcurrentHashMap<>());
            stringMutablePairMap.computeIfAbsent(payload.getEventPrefix() + payload.getSignature(), m ->
                    MutablePair.of(System.currentTimeMillis(), -1L));

            eventList.add(payload.getEventPrefix() + payload.getSignature());

            LOG.debug("Updated time of payload before sending process: " + payload.getEventPrefix() + payload.getSignature());
        });
        return eventList;
    }

    @Suspendable
    private List<String> updateStartTime(final List<IGrapheneWritePayload> iGrapheneWritePayloads,
                                         final Transaction transaction,
                                         final ClientObject clientObject) {
        List<IGrapheneWritePayload> payloadsToRemove = new ArrayList<>();
        List<String> eventList = new ArrayList<>();

        //List<IGrapheneWritePayload> iGrapheneWritePayloadsCcme = new Cloner().deepClone(iGrapheneWritePayloads);

        //transaction.getOperations().parallelStream().forEach( operation ->

        for (final BaseOperation operation : transaction.getOperations()) {
            for (final IGrapheneWritePayload payload : iGrapheneWritePayloads) {
                if (operation.toJsonString().contains(payload.getSignature())) {

                    Map<String, MutablePair<Long, Long>> valueMap =
                            GrapheneSubscription.getObtainedEventsMap().get(clientObject.getClientId());

                    valueMap.get(payload.getEventPrefix() + payload.getSignature()).setLeft(System.nanoTime());

                    Map<String, MutablePair<Long, Long>> stringMutablePairMap =
                            ListenerReferenceValues.getTimeMap().computeIfAbsent(clientObject.getClientId(),
                                    c -> new ConcurrentHashMap<>());
                    stringMutablePairMap.computeIfAbsent(payload.getEventPrefix() + payload.getSignature(), m ->
                            MutablePair.of(System.currentTimeMillis(), -1L));

                    eventList.add(payload.getEventPrefix() + payload.getSignature());

                    payloadsToRemove.add(payload);
                    LOG.debug("Updated time of payload before sending process: " + payload.getEventPrefix() + payload.getSignature());
                }
            }

                    /*Supplier<Stream<IGrapheneWritePayload>> iGrapheneWritePayloadStream = () ->
                            iGrapheneWritePayloads.parallelStream().filter(p ->
                                    operation.toJsonString().contains(p.getSignature()));

                    if (iGrapheneWritePayloadStream.get().count() != 1) {
                        LOG.error("iGrapheneWritePayloadStream.count() != 1, " + iGrapheneWritePayloadStream.get()
                        .count());
                    }

                    Optional<IGrapheneWritePayload> any = iGrapheneWritePayloadStream.get().findAny();

                    if (any.isPresent()) {
                        IGrapheneWritePayload payload = any.get();
                        Map<String, MutablePair<Long, Long>> valueMap =
                                GrapheneSubscription.getObtainedEventsMap().get(clientObject.getClientId());

                        valueMap.get(payload.getEventPrefix() + payload.getSignature()).setLeft(System.nanoTime());

                        Map<String, MutablePair<Long, Long>> stringMutablePairMap =
                                ListenerReferenceValues.getTimeMap().computeIfAbsent(clientObject.getClientId(),
                                        c -> new ConcurrentHashMap<>());
                        stringMutablePairMap.computeIfAbsent(payload.getEventPrefix() + payload.getSignature(), m ->
                                MutablePair.of(System.currentTimeMillis(), -1L));

                        eventList.add(payload.getEventPrefix() + payload.getSignature());

                        payloadsToRemove.add(payload);
                        LOG.debug("Updated time of payload before sending process: " + payload.getEventPrefix() +
                        payload.getSignature());
                    } else {
                        LOG.error("No event found");
                    }*/

        }

        /*operation -> iGrapheneWritePayloadsCcme.forEach(payload -> {
                    if (operation.toJsonString().contains(payload.getSignature())) {
                        Map<String, MutablePair<Long, Long>> valueMap =
                                GrapheneSubscription.getObtainedEventsMap().get(clientObject.getClientId());

                        valueMap.get(payload.getEventPrefix() + payload.getSignature()).setLeft(System.nanoTime());

                        Map<String, MutablePair<Long, Long>> stringMutablePairMap =
                                ListenerReferenceValues.getTimeMap().computeIfAbsent(clientObject.getClientId(),
                                        c -> new ConcurrentHashMap<>());
                        stringMutablePairMap.computeIfAbsent(payload.getEventPrefix() + payload.getSignature(), m ->
                                MutablePair.of(System.currentTimeMillis(), -1L));

                        eventList.add(payload.getEventPrefix() + payload.getSignature());

                        payloadsToRemove.add(payload);
                        LOG.debug("Updated time of payload before sending process: " + payload.getEventPrefix() +
                        payload.getSignature());
                    }
                }
        )
        );*/
        iGrapheneWritePayloads.removeIf(payloadsToRemove::contains);
        /*Iterator<IGrapheneWritePayload> iterator = iGrapheneWritePayloads.iterator();
        while (iterator.hasNext()) {
            if (payloadsToRemove.contains(iterator.next())) {
                iterator.remove();
            }
        }*/
        //iGrapheneWritePayloads.removeAll(payloadsToRemove);
        return eventList;
    }

    @Suspendable
    private void updateContainedPayloadTypeByMap(final Transaction transaction,
                                                 final WriteStatisticObject writeStatisticObject) {

        transaction.getOperations().forEach(operation -> {
            IGrapheneWritePayload payload = this.transactionDispatcher.getPayloadMapping().get(operation);
            writeStatisticObject.getSpecificPayloadTypeList().add(payload.getSpecificPayloadType());
        });

    }

    @Suspendable
    private void updateContainedPayloadType(final List<IGrapheneWritePayload> iGrapheneWritePayloads,
                                            final Transaction transaction,
                                            final WriteStatisticObject writeStatisticObject) {
        List<IGrapheneWritePayload> payloadsToRemove = new ArrayList<>();
        //transaction.getOperations().parallelStream().forEach( operation -> {

        for (final BaseOperation operation : transaction.getOperations()) {
            for (final IGrapheneWritePayload payload : iGrapheneWritePayloads) {
                if (operation.toJsonString().contains(payload.getSignature())) {
                    writeStatisticObject.getSpecificPayloadTypeList().add(payload.getSpecificPayloadType());
                    payloadsToRemove.add(payload);
                }
            }

                    /*Supplier<Stream<IGrapheneWritePayload>> iGrapheneWritePayloadStream = () ->
                            iGrapheneWritePayloads.parallelStream().filter(p ->
                                    operation.toJsonString().contains(p.getSignature()));

                    if (iGrapheneWritePayloadStream.get().count() != 1) {
                        LOG.error("iGrapheneWritePayloadStream.count() (payloadType) != 1, " +
                        iGrapheneWritePayloadStream.get().count());
                    }

                    Optional<IGrapheneWritePayload> any = iGrapheneWritePayloadStream.get().findAny();

                    if (any.isPresent()) {
                        IGrapheneWritePayload payload = any.get();
                        writeStatisticObject.getSpecificPayloadTypeList().add(payload.getSpecificPayloadType());
                        payloadsToRemove.add(payload);
                    } else {
                        LOG.error("No event found");
                    }*/

        }

                /*operation -> iGrapheneWritePayloads.forEach(payload -> {
                            if (operation.toJsonString().contains(payload.getSignature())) {
                                writeStatisticObject.getSpecificPayloadTypeList().add(payload.getSpecificPayloadType());
                                payloadsToRemove.add(payload);
                            }
                        }
                )
        );*/

        iGrapheneWritePayloads.removeIf(payloadsToRemove::contains);
        //iGrapheneWritePayloads.removeAll(payloadsToRemove);

    }

    @Suspendable
    private static synchronized void disconnectWebsockets(final ClientObject clientObject,
                                                          final PrepareGrapheneWorkloadObject prepareWorkloadObject) {
        if (Configuration.DISCONNECT_WEBSOCKETS) {
            for (final WebSocket webSocket : prepareWorkloadObject.getWebsocketList()) {
                webSocket.disconnect();
                LOG.info("Closed websocket, finished " + clientObject.getClientId());
            }
        }
    }

    @Suspendable
    private void awaitEndOfExecution(final PrepareGrapheneWorkloadObject prepareWorkloadObject) {
        for (final IListenerDisconnectionLogic iListenerDisconnectionLogic :
                prepareWorkloadObject.getIListenerDisconnectionLogicList()) {
            try {
                iListenerDisconnectionLogic.isDone().get(Configuration.TIMEOUT_LISTENER,
                        Configuration.TIMEOUT_LISTENER_TIME_UNIT);
                iStatistics.addAll(iListenerDisconnectionLogic.getStatistics());
            } catch (InterruptedException | ExecutionException | TimeoutException ex) {
                ExceptionHandler.logException(ex);
            }
        }
    }

    @Suspendable
    public void read(final List<IGrapheneReadPayload> iGrapheneReadPayloads, final ClientObject clientObject,
                     final int workloadId) {

        RateLimiter rateLimiter = Configuration.ENABLE_RATE_LIMITER_FOR_READ_PAYLOADS ?
                RateLimiter.create(
                        GenericSelectionStrategy.selectFixed(
                                Configuration.READ_PAYLOADS_PER_SECOND, Collections.singletonList(0), false).get(0)
                ) : null;

        for (final IGrapheneReadPayload iGrapheneReadPayload : iGrapheneReadPayloads) {
            if (Configuration.ENABLE_RATE_LIMITER_FOR_READ_PAYLOADS) {
                rateLimiter.acquire();
            }

            ReadStatisticObject readStatisticObject = new ReadStatisticObject();
            readStatisticObject.setCurrentTimeStart(System.currentTimeMillis());
            read(iGrapheneReadPayload, readStatisticObject);
            readStatisticObject.setCurrentTimeEnd(System.currentTimeMillis());
            readStatisticObject.setRequestNumber(++readRequests);
            readStatisticObject.setClientId(clientObject.getClientId());
            readStatisticObject.setRequestId("clid-" + clientObject.getClientId() + "-read-" + readRequests + "-wlid" +
                    "-" + workloadId);
            readStatisticObject.getSpecificPayloadTypeList().add(iGrapheneReadPayload.getSpecificPayloadType());
            iStatistics.add(readStatisticObject);

        }

    }

    @Suspendable
    private void read(final IGrapheneReadPayload iGrapheneReadPayload,
                      final ReadStatisticObject readStatisticObject) {

        IReadingMethod read;
        if (Configuration.READ_PAYLOAD_TYPE == ReadPayloadType.JSON_RPC) {
            read = new Read();
        } else if (Configuration.READ_PAYLOAD_TYPE == ReadPayloadType.NODE) {
            read = new ReadByObject(false,
                    new GrapheneWitnessListener().registerWitnessResponseListener(),
                    iGrapheneReadPayload.getPayload(), readStatisticObject);
        } else if (Configuration.READ_PAYLOAD_TYPE == ReadPayloadType.WALLET) {
            read = new ReadByObjectWallet(false,
                    new GrapheneWitnessListener().registerWitnessResponseListener(),
                    iGrapheneReadPayload.getPayload(), readStatisticObject);
        } else {
            throw new NotYetImplementedException("Not yet implemented");
        }

        boolean hasError;
        String hasMessage;
        int e = 0;
        int retries = Configuration.RESEND_TIMES_UPON_ERROR_READ;
        do {
            ImmutablePair<Boolean, String> readRes = null;

            if (Configuration.READ_PAYLOAD_TYPE == ReadPayloadType.JSON_RPC) {
                List<String> serversRead = GenericSelectionStrategy.selectFixed(Helper.getAccounts(false),
                        Collections.singletonList(0), false);

                String server = serversRead.get(0);
                readStatisticObject.getParticipatingServers().add(server);

                readRes = (ImmutablePair<Boolean, String>) read.read(iGrapheneReadPayload.getPayload(),
                        server, readStatisticObject);
            } else if (Configuration.READ_PAYLOAD_TYPE == ReadPayloadType.NODE || Configuration.READ_PAYLOAD_TYPE == ReadPayloadType.WALLET) {
                List<String> serversRead;
                if (Configuration.READ_PAYLOAD_TYPE == ReadPayloadType.NODE) {
                    serversRead = GenericSelectionStrategy.selectFixed(Helper.getAccounts(false),
                            Collections.singletonList(0), false);
                } else if (Configuration.READ_PAYLOAD_TYPE == ReadPayloadType.WALLET) {
                    serversRead = GenericSelectionStrategy.selectFixed(Helper.getAccounts(true),
                            Collections.singletonList(0), false);
                } else {
                    throw new NotYetImplementedException("Not yet implemented");
                }

                String server = serversRead.get(0);
                readStatisticObject.getParticipatingServers().add(server);

                WebSocket webSocketRead =
                        GrapheneWebsocket.prepareWebsocket(GrapheneWebsocket.prepareWebsocketFactory(),
                                server);

                if (Configuration.READ_PAYLOAD_TYPE == ReadPayloadType.NODE) {
                    ReadByObject readByObject = (ReadByObject) read;

                    webSocketRead.addListener(readByObject);
                    try {
                        webSocketRead.connect();
                        readRes = readByObject.getDone().get(Configuration.TIMEOUT_TRANSACTION,
                                Configuration.TIMEOUT_UNIT_TRANSACTION);
                    } catch (WebSocketException | InterruptedException | ExecutionException ex) {
                        ExceptionHandler.logException(ex);
                    } catch (TimeoutException ex) {
                        ExceptionHandler.logException(ex);
                        readStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                    } finally {
                        if (Configuration.CLOSE_SOCKET_AFTER_READ) {
                            webSocketRead.disconnect();
                        }
                    }
                } else {
                    ReadByObjectWallet readByObjectWallet = (ReadByObjectWallet) read;

                    webSocketRead.addListener(readByObjectWallet);
                    try {
                        webSocketRead.connect();
                        readRes = readByObjectWallet.getDone().get(Configuration.TIMEOUT_TRANSACTION,
                                Configuration.TIMEOUT_UNIT_TRANSACTION);
                    } catch (WebSocketException | InterruptedException | ExecutionException ex) {
                        ExceptionHandler.logException(ex);
                    } catch (TimeoutException ex) {
                        ExceptionHandler.logException(ex);
                        readStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                    } finally {
                        if (Configuration.CLOSE_SOCKET_AFTER_READ) {
                            webSocketRead.disconnect();
                        }
                    }
                }

            } else {
                throw new NotYetImplementedException("Not yet implemented");
            }

            hasError = Objects.requireNonNull(readRes).getLeft();
            hasMessage = readRes.getRight();

            if (hasError) {
                LOG.error("Had error (read) resend " + e + " message " + hasMessage);
                e++;
            }

            if (hasError && hasMessage != null) {
                readStatisticObject.getErrorMessages().add(hasMessage);
            }

            if (Configuration.DROP_ON_TIMEOUT && readRes.getRight().contains("TIMEOUT_EX")) {
                LOG.error("Dropping read request due to exception " + readRes.getRight());
                break;
            }

        } while (hasError && e < retries);
        LOG.info("Number of resends (read): " + e);

        if (hasError) {
            readStatisticObject.setFailedRequest(true);
        }
    }

    @Suspendable
    private void write(final Transaction transaction, final Write write,
                       final WriteStatisticObject writeStatisticObject,
                       final PrepareGrapheneWorkloadObject prepareWorkloadObject, final ClientObject clientObject) {

        /*GenericSelectionStrategy.selectFixed(GrapheneHelper.getAccounts(Configuration
                .IS_TRANSACTION_EXECUTED_BY_WALLET),
                        Collections.singletonList(0), false);*/

        boolean hasError;
        String hasMessage;
        int e = 0;
        boolean timeSet = false;
        int retries = Configuration.RESEND_TIMES_UPON_ERROR_WRITE;
        do {

            String server;

            URI uri = null;
            try {
                uri = new URI("ws://" + prepareWorkloadObject.getKeyServerAndAccountList().get(0).getMiddle());
            } catch (URISyntaxException ex) {
                ExceptionHandler.logException(ex);
            }

            if (Configuration.IS_TRANSACTION_EXECUTED_BY_WALLET) {
                server =
                        Objects.requireNonNull(uri).getScheme() + "://" + uri.getHost() + ":" + (uri.getPort() + WALLET_OFFSET);
            } else {
                server = Objects.requireNonNull(uri).getScheme() + "://" + uri.getHost() + ":" + (uri.getPort());
            }

            writeStatisticObject.getParticipatingServers().add(server);

            WebSocket webSocket = GrapheneWebsocket.prepareWebsocket(GrapheneWebsocket.prepareWebsocketFactory(),
                    server);

            if (Configuration.ENABLE_RATE_LIMITER_FOR_WRITE_PAYLOADS) {
                rateLimiter.acquire(Configuration.NUMBER_OF_OPERATIONS_PER_TRANSACTION);
                if (GeneralConfiguration.NOTE_RATE_LIMITER_WRITE == client.statistics.WriteStatisticObject.NoteRateLimiter.YES) {
                    if(!timeSet) {
                        writeStatisticObject.setCurrentTimeStart(System.currentTimeMillis());
                        updateStartTimeByMap(transaction, clientObject);
                        timeSet = true;
                    }
                }
            }

            ImmutablePair<Boolean, String> writeRes = write.write(transaction, false, webSocket,
                    writeStatisticObject, WALLET_OFFSET);

            hasError = writeRes.getLeft();
            hasMessage = writeRes.getRight();

            if (hasError) {
                LOG.error("Had error (write) resend " + e + " message " + hasMessage);
                e++;
            }

            if (hasError && hasMessage != null) {
                writeStatisticObject.getErrorMessages().add(hasMessage);
            }

            if (Configuration.DROP_ON_BAD_ALLOC && writeRes.getRight().contains("bad_alloc") || Configuration.DROP_ON_TIMEOUT && writeRes.getRight().contains("TIMEOUT_EX") || Configuration.DROP_ON_UNIQUE_CONSTRAINT && writeRes.getRight().contains("Could not create object! Most likely a uniqueness constraint is violated.")) {
                LOG.error("Dropping write request due to exception " + writeRes.getRight());
                break;
            }

        } while (hasError && e < retries);
        LOG.info("Number of resends (write): " + e);

        if (hasError) {
            writeStatisticObject.setFailedRequest(true);
        }
    }

    @Suspendable
    private List<IGrapheneWritePayload> prepareWritePayloads(final ClientObject clientObject,
                                                             final List<PrepareGrapheneWorkloadObject> listOfWorkloadObjects,
                                                             final String acctId) {
        List<IGrapheneWritePayload> iGrapheneWritePayloads;
        if (Configuration.PREPARE_WRITE_PAYLOADS) {
            String id = clientObject.getClientId() + WRITE_SUFFIX;
            List<List<IGrapheneWritePayload>> grapheneWritePayloads =
                    listOfWorkloadObjects.get(0).getGrapheneWritePayloads();
            iGrapheneWritePayloads = GenericSelectionStrategy.selectRoundRobin(grapheneWritePayloads, 1, false, false,
                    id, 1, false).get(0);
        } else {

            IGraphenePayloads iGrapheneWritePayloadPattern = null;
            try {
                iGrapheneWritePayloadPattern =
                        Configuration.WRITE_PAYLOAD_PATTERN.getDeclaredConstructor().newInstance();
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
                ExceptionHandler.logException(ex);
            }

            iGrapheneWritePayloads =
                    (List<IGrapheneWritePayload>) iGrapheneWritePayloadPattern.getPayloads(clientObject, acctId,
                            Configuration.NUMBER_OF_TRANSACTION_PAYLOADS_PER_CLIENT);
        }
        return iGrapheneWritePayloads;
    }

    @Suspendable
    private List<IGrapheneReadPayload> prepareReadPayloads(final ClientObject clientObject,
                                                           final List<PrepareGrapheneWorkloadObject> listOfWorkloadObjects) {
        List<IGrapheneReadPayload> iGrapheneReadPayloads;
        if (Configuration.PREPARE_READ_PAYLOADS) {
            String id = clientObject.getClientId() + READ_SUFFIX;
            List<List<IGrapheneReadPayload>> grapheneReadPayloads =
                    listOfWorkloadObjects.get(0).getGrapheneReadPayloads();
            iGrapheneReadPayloads = GenericSelectionStrategy.selectRoundRobin(grapheneReadPayloads, 1, false, false,
                    id, 1, false).get(0);
        } else {

            IGraphenePayloads iGrapheneReadPayloadPattern = null;
            try {
                iGrapheneReadPayloadPattern = Configuration.READ_PAYLOAD_PATTERN.getDeclaredConstructor().newInstance();
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
                ExceptionHandler.logException(ex);
            }

            iGrapheneReadPayloads =
                    (List<IGrapheneReadPayload>) iGrapheneReadPayloadPattern.getPayloads(clientObject);
        }
        return iGrapheneReadPayloads;
    }

    @Suspendable
    private void dispatch(final List<IGrapheneWritePayload> iGrapheneWritePayloads,
                          final long numberOfOperationsPerTransaction, int transactionCounter,
                          final List<List<BaseOperation>> operationList, final ClientObject clientObject) {

        List<IGrapheneWritePayload> iGrapheneWritePayloadsTmp = new ArrayList<>(iGrapheneWritePayloads);

        if (Configuration.DISPATCH_OPERATIONS_TO_TRANSACTIONS) {
            int j = 0;

            try {
                this.transactionDispatcher =
                        Configuration.I_OPERATION_TO_TRANSACTION_DISPATCHER.getDeclaredConstructor().newInstance();

                while (iGrapheneWritePayloadsTmp.size() != 0) {
                    LOG.info("Remaining payloads: " + iGrapheneWritePayloadsTmp.size());
                    LOG.info("Number of operations per " +
                            "transaction: " + numberOfOperationsPerTransaction);

                    List<BaseOperation> baseOperationList =
                            transactionDispatcher.dispatchOperations(iGrapheneWritePayloadsTmp,
                                    clientObject);
                    operationList.add(baseOperationList);

                    iGrapheneWritePayloadsTmp.subList(0, operationList.get(j).size()).clear();

                    transactionCounter++;

                    LOG.info("Current number of payloads: " + iGrapheneWritePayloadsTmp.size() + ", " + " Current " +
                            "transaction" +
                            " " +
                            "counter: " + transactionCounter
                            + ", " + " Current number of operations in transaction: " + operationList.get(j).size());

                    j++;

                }
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
                ExceptionHandler.logException(ex);
            }
        } else {
            List<BaseOperation> operationListTemp = new ArrayList<>();
            try {
                this.transactionDispatcher =
                        Configuration.I_OPERATION_TO_TRANSACTION_DISPATCHER.getDeclaredConstructor().newInstance();
                for (final IGrapheneWritePayload iGrapheneWritePayload : iGrapheneWritePayloadsTmp) {
                    operationListTemp.add(iGrapheneWritePayload.getPayload());
                    operationList.add(operationListTemp);

                    transactionDispatcher.getPayloadMapping().put(iGrapheneWritePayload.getPayload(),
                            iGrapheneWritePayload);

                    GrapheneSubscription.getObtainedEventsMap().get(clientObject.getClientId()).get(iGrapheneWritePayload.getEventPrefix() + iGrapheneWritePayload.getSignature()).setLeft(System.nanoTime());

                }
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
                ExceptionHandler.logException(ex);
            }
        }
    }

    @Suspendable
    private void prepareExpectedEventMap(final ClientObject clientObject,
                                         final List<IGrapheneWritePayload> iGrapheneWritePayloads) {

        for (final IGrapheneWritePayload iGrapheneWritePayload : iGrapheneWritePayloads) {
            String expectedEvent = iGrapheneWritePayload.getEventPrefix() + iGrapheneWritePayload.getSignature();

            synchronized (GrapheneSubscription.getObtainedEventsMap()) {
                if (GrapheneSubscription.getObtainedEventsMap().get(clientObject.getClientId()) == null) {
                    GrapheneSubscription.getObtainedEventsMap().put(clientObject.getClientId(),
                            java.util.Collections.synchronizedMap(new HashMap<>()));
                    GrapheneSubscription.getObtainedEventsMap().get(clientObject.getClientId()).put(
                            expectedEvent,
                            MutablePair.of(System.nanoTime(), -1L));
                    LOG.debug("Put: " + expectedEvent);
                } else {
                    GrapheneSubscription.getObtainedEventsMap().get(clientObject.getClientId()).put(
                            expectedEvent,
                            MutablePair.of(System.nanoTime(), -1L));
                    LOG.debug("Set: " + expectedEvent);
                }
            }
        }
    }

    @Suspendable
    private void debugDispatcherValues(final List<IGrapheneWritePayload> iGrapheneWritePayloads) {
        if (iGrapheneWritePayloads.size() != (Configuration.NUMBER_OF_TRANSACTIONS_PER_CLIENT * Configuration.NUMBER_OF_OPERATIONS_PER_TRANSACTION)) {
            LOG.info("Number of payloads not equal to set operations per transaction, set dispatcher: " + Configuration.I_OPERATION_TO_TRANSACTION_DISPATCHER.getName());
        }
        if (iGrapheneWritePayloads.size() > (Configuration.NUMBER_OF_TRANSACTIONS_PER_CLIENT * Configuration.NUMBER_OF_OPERATIONS_PER_TRANSACTION)) {
            LOG.info("More payloads than transactions to dispatch");
        }
    }

    @Suspendable
    private void addToExpectedEventMap(final ClientObject clientObject,
                                       final String signature,
                                       final String expectedEventPrefix) {

        String expectedEvent = expectedEventPrefix + signature;

        Map<String, MutablePair<Long, Long>> stringMutablePairMap =
                GrapheneSubscription.getObtainedEventsMap().computeIfAbsent(clientObject.getClientId(),
                        c -> new ConcurrentHashMap<>());
        stringMutablePairMap.computeIfAbsent(expectedEvent, m ->
                MutablePair.of(System.nanoTime(), -1L));

    }

    @SafeVarargs
    @Suspendable
    @Override
    public final <E> E endWorkload(final E... params) {
        LOG.info(((ClientObject) params[1]).getClientId() + " client ended");
        return null;
    }

    @SafeVarargs
    @Override
    @Suspendable
    public synchronized final <E> Queue<IStatistics> getStatistics(final E... params) {
        return iStatistics;
    }

    @SafeVarargs
    @Override
    @Suspendable
    public final <E> void handleRequestDistribution(final E... params) {
        try {
            int randomSleep = NumberGenerator.selectRandomAsInt(50, 250);
            LOG.debug("Sleep time: " + randomSleep + " for " + params[0]);
            Strand.sleep(randomSleep);
        } catch (SuspendExecution | InterruptedException ex) {
            ExceptionHandler.logException(ex);
        }
    }
}
