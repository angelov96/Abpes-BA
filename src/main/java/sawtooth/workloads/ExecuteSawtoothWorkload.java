package sawtooth.workloads;

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
import co.nstant.in.cbor.CborDecoder;
import co.nstant.in.cbor.CborException;
import co.nstant.in.cbor.model.DataItem;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.fibers.Suspendable;
import co.paralleluniverse.strands.Strand;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.log4j.Logger;
import org.hibernate.cfg.NotYetImplementedException;
import org.zeromq.ZMQ;
import sawtooth.components.SawtoothBatchUtils;
import sawtooth.components.SawtoothTransactionUtils;
import sawtooth.configuration.Configuration;
import sawtooth.connection.ConnectionEnum;
import sawtooth.connection.SocketCreationEnum;
import sawtooth.connection.ZmqConnection;
import sawtooth.listener.UpdateMeasureTimeType;
import sawtooth.listener.WebsocketListener;
import sawtooth.listener.ZmqListener;
import sawtooth.payload_patterns.ISawtoothPayloads;
import sawtooth.payload_patterns.ITransactionToBatchDispatcher;
import sawtooth.payloads.ISawtoothReadPayload;
import sawtooth.payloads.ISawtoothWritePayload;
import sawtooth.read.IReadingMethod;
import sawtooth.read.ReadWebsocket;
import sawtooth.read.ReadZmq;
import sawtooth.sdk.protobuf.Batch;
import sawtooth.sdk.protobuf.BatchList;
import sawtooth.sdk.protobuf.Transaction;
import sawtooth.sdk.signing.Signer;
import sawtooth.statistics.ReadStatisticObject;
import sawtooth.statistics.WriteStatisticObject;
import sawtooth.write.WriteWebsocket;
import sawtooth.write.WriteZmq;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class ExecuteSawtoothWorkload implements IExecuteWorkload, IRequestDistribution {

    private static final Logger LOG = Logger.getLogger(ExecuteSawtoothWorkload.class);
    private static final String READ_SUFFIX = "-read";
    private static final String WRITE_SUFFIX = "-write";
    private final Queue<IStatistics> iStatistics = new ConcurrentLinkedQueue<>();
    private int writeRequests;
    private int readRequests;
    private ITransactionToBatchDispatcher transactionDispatcher;

    @SafeVarargs
    @Suspendable
    @Override
    public final <E> E executeWorkload(final E... params) {

        ClientObject clientObject = (ClientObject) params[1];
        int workloadId = Integer.parseInt(String.valueOf(params[2])) + 1;

        List<PrepareSawtoothWorkloadObject> listOfWorkloadObjects =
                GenericSelectionStrategy.selectFixed(((ArrayList<PrepareSawtoothWorkloadObject>) params[0]),
                        Collections.singletonList(0), false);
        PrepareSawtoothWorkloadObject prepareWorkloadObject = listOfWorkloadObjects.get(0);

        if (Configuration.SEND_WRITE_REQUESTS) {
            List<ISawtoothWritePayload> sawtoothWritePayloads = prepareWritePayloads(clientObject,
                    listOfWorkloadObjects);

            Signer signer = prepareWorkloadObject.getSigner();

            long numberOfBatchesToSend = Configuration.NUMBER_OF_BATCHES_PER_CLIENT;
            long numberOfTransactionsPerBatch = Configuration.NUMBER_OF_TRANSACTIONS_PER_BATCH_PER_CLIENT;

            BatchList batchListToSend = BatchList.getDefaultInstance();
            int batchCounter = 0;

            debugDispatcherValues(sawtoothWritePayloads);

            prepareExpectedEventMap(clientObject, sawtoothWritePayloads);

            batchListToSend = dispatch(sawtoothWritePayloads, signer, numberOfBatchesToSend,
                    numberOfTransactionsPerBatch
                    , batchListToSend, batchCounter, clientObject);

            prepareWrite(clientObject, prepareWorkloadObject, signer, batchListToSend, sawtoothWritePayloads,
                    workloadId);
        }

        if (Configuration.SEND_READ_REQUESTS) {
            List<ISawtoothReadPayload> sawtoothReadPayloads = prepareReadPayloads(clientObject, listOfWorkloadObjects);
            read(clientObject, prepareWorkloadObject, sawtoothReadPayloads, workloadId);
        }

        // awaitEndOfExecution(prepareWorkloadObject);

        // disconnectListeners(clientObject, prepareWorkloadObject);

        return null;
    }

    @Suspendable
    private void read(final ClientObject clientObject, final PrepareSawtoothWorkloadObject prepareWorkloadObject,
                      final List<ISawtoothReadPayload> iSawtoothReadPayloads, final int workloadId) {

        RateLimiter rateLimiter = Configuration.ENABLE_RATE_LIMITER_FOR_READ_PAYLOADS ?
                RateLimiter.create(
                        GenericSelectionStrategy.selectFixed(
                                Configuration.READ_PAYLOADS_PER_SECOND,
                                Collections.singletonList(0), false).get(0)
                ) : null;

        ZmqConnection zmqConnection = null;
        ZMQ.Socket socket = null;
        if (Configuration.SOCKET_CREATION_ENUM == SocketCreationEnum.BY_WORKLOAD && Configuration.CONNECTION_TYPE_WRITE == ConnectionEnum.ZMQ) {
            zmqConnection = new ZmqConnection();
            socket = zmqConnection.createZmqListener();
        }

        for (final ISawtoothReadPayload iSawtoothReadPayload : iSawtoothReadPayloads) {

            if (Configuration.ENABLE_RATE_LIMITER_FOR_READ_PAYLOADS) {
                rateLimiter.acquire();
            }

            ReadStatisticObject readStatisticObject = new ReadStatisticObject();
            readStatisticObject.setCurrentTimeStart(System.currentTimeMillis());
            read(clientObject, prepareWorkloadObject, iSawtoothReadPayload, readStatisticObject,
                    zmqConnection, socket);
            readStatisticObject.setCurrentTimeEnd(System.currentTimeMillis());
            readStatisticObject.setRequestNumber(++readRequests);
            readStatisticObject.setClientId(clientObject.getClientId());
            readStatisticObject.setRequestId("clid-" + clientObject.getClientId() + "-read-" + readRequests + "-wlid" +
                    "-" + workloadId);
            readStatisticObject.getSpecificPayloadTypeList().add(iSawtoothReadPayload.getSpecificPayloadType());
            iStatistics.add(readStatisticObject);
        }

        if (socket != null && socket.getLastEndpoint() != null) {
            boolean disconnect = socket.disconnect(socket.getLastEndpoint());
            socket.close();
            LOG.debug("Socket disconnected: " + disconnect);
        }

    }

    @Suspendable
    private void read(final ClientObject clientObject, final PrepareSawtoothWorkloadObject prepareWorkloadObject,
                      final ISawtoothReadPayload iSawtoothReadPayload
            , final ReadStatisticObject readStatisticObject, final ZmqConnection zmqConnection,
                      final ZMQ.Socket socket) {

        IReadingMethod read;
        if (Configuration.CONNECTION_TYPE_READ == ConnectionEnum.ZMQ) {
            read = new ReadZmq();
        } else if (Configuration.CONNECTION_TYPE_READ == ConnectionEnum.WebSocket) {
            read = new ReadWebsocket();
        } else {
            throw new NotYetImplementedException("Not yet implemented");
        }

        boolean hasError;
        String hasMessage;
        int e = 0;
        int retries = Configuration.RESEND_TIMES_UPON_ERROR_READ;
        do {
            ImmutablePair<Boolean, String> readRes;

            if (Configuration.CONNECTION_TYPE_READ == ConnectionEnum.ZMQ) {

                ZmqConnection zmqConnectionCopy = zmqConnection;
                ZMQ.Socket socketCopy = socket;
                if (Configuration.SOCKET_CREATION_ENUM == SocketCreationEnum.BY_ACTION) {
                    zmqConnectionCopy = new ZmqConnection();
                    socketCopy = zmqConnectionCopy.createZmqListener();
                }

                String serverAddress =
                        GenericSelectionStrategy.selectFixed(Arrays.asList(Configuration.VALIDATORS_TO_SEND_TRANSACTIONS_TO_ZMQ),
                                Collections.singletonList(0), false).get(0);

                readStatisticObject.getParticipatingServers().add(serverAddress);

                zmqConnectionCopy.connectToZmq(socketCopy, serverAddress);

                readRes =
                        read.read(iSawtoothReadPayload, socketCopy, clientObject.getClientId() +
                                        "-read",
                                readStatisticObject);

                if (Configuration.SOCKET_CREATION_ENUM == SocketCreationEnum.BY_ACTION) {
                    boolean disconnect = socketCopy.disconnect(socketCopy.getLastEndpoint());
                    socketCopy.close();
                    LOG.debug("Socket disconnected: " + disconnect);
                }

            } else if (Configuration.CONNECTION_TYPE_READ == ConnectionEnum.WebSocket) {
                String serverAddress =
                        GenericSelectionStrategy.selectFixed(Arrays.asList(Configuration.VALIDATORS_TO_SEND_TRANSACTIONS_TO_WEBSOCKET),
                                Collections.singletonList(0), false).get(0);

                readStatisticObject.getParticipatingServers().add(serverAddress);

                ReadWebsocket readWebsocket;
                if (Configuration.USE_PREPARED_READ_WEBSOCKET) {
                    readWebsocket = prepareWorkloadObject.getReadWebsocket();
                } else {
                    readWebsocket = (ReadWebsocket) read;
                }

                readRes = readWebsocket.read(serverAddress,
                        iSawtoothReadPayload, readStatisticObject);
            } else {
                throw new NotYetImplementedException("Not yet implemented");
            }

            hasError = readRes.getLeft();
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
    private void awaitEndOfExecution(final PrepareSawtoothWorkloadObject prepareWorkloadObject) {
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
    private static synchronized void disconnectListeners(final ClientObject clientObject,
                                                         final PrepareSawtoothWorkloadObject prepareWorkloadObject) {
        if (Configuration.DISCONNECT_LISTENERS) {
            for (final String webSocketSubscriptionServer : prepareWorkloadObject.getWebSocketSubscriptionServers()) {
                WebsocketListener websocketListener =
                        new WebsocketListener();
                websocketListener.createWebsocketListener(webSocketSubscriptionServer, false);
                LOG.info("Closed websocket, finished " + clientObject.getClientId());
            }
            for (final Map.Entry<String, ZMQ.Socket> socketEntry :
                    prepareWorkloadObject.getZmqSocketSubscriptionServerMap().entrySet()) {
                ZmqListener.unsubscribeListener(socketEntry.getValue(), socketEntry.getKey());
                LOG.info("Closed zmq listener, finished " + clientObject.getClientId());
            }

        }
    }

    private int bn;

    private static final RateLimiter rateLimiter = Configuration.ENABLE_RATE_LIMITER_FOR_WRITE_PAYLOADS ?
            RateLimiter.create(
                    GenericSelectionStrategy.selectFixed(
                            Configuration.WRITE_PAYLOADS_PER_SECOND, Collections.singletonList(0), false).get(0)
            ) : null;

    @Suspendable
    private void prepareWrite(final ClientObject clientObject, final PrepareSawtoothWorkloadObject prepareWorkloadObject
            , final Signer signer, final BatchList batchListToSend,
                              final List<ISawtoothWritePayload> iSawtoothWritePayloads, final int workloadId) {

        /*RateLimiter rateLimiter = Configuration.ENABLE_RATE_LIMITER_FOR_WRITE_PAYLOADS ?
                RateLimiter.create(
                        GenericSelectionStrategy.selectFixed(
                                Configuration.WRITE_PAYLOADS_PER_SECOND, Collections.singletonList(0), false).get(0)
                ) : null;*/

        ZmqConnection zmqConnection = null;
        ZMQ.Socket socket = null;
        if (Configuration.SOCKET_CREATION_ENUM == SocketCreationEnum.BY_WORKLOAD && Configuration.CONNECTION_TYPE_WRITE == ConnectionEnum.ZMQ) {
            zmqConnection = new ZmqConnection();
            socket = zmqConnection.createZmqListener();
        }

        if (Configuration.SEND_BATCH_BY_BATCH) {
            BatchList build;
            for (final Batch batch : batchListToSend.getBatchesList()) {
                if (Configuration.SEND_TRANSACTION_BY_TRANSACTION) {
                    for (final Transaction transaction : batch.getTransactionsList()) {

                        /*if (Configuration.ENABLE_RATE_LIMITER_FOR_WRITE_PAYLOADS) {
                            rateLimiter.acquire();
                        }*/

                        Batch batchToSend =
                                SawtoothBatchUtils.prepareBatch(Collections.singletonList(transaction),
                                        signer);
                        build = BatchList.newBuilder().addBatches(batchToSend).build();

                        List<String> eventList = new ArrayList<>();
                        if (Configuration.UPDATE_MEASURE_TIME == UpdateMeasureTimeType.BY_TRANSACTION) {
                            eventList = updateStartTimeByMap(build,
                                    clientObject);
                        }

                        //handleRequestDistribution(clientObject.getClientId());

                        WriteStatisticObject writeStatisticObject = new WriteStatisticObject();

                        writeStatisticObject.getAssocEventList().addAll(eventList);

                        writeStatisticObject.setCurrentTimeStart(System.currentTimeMillis());
                        write(clientObject, prepareWorkloadObject, build, writeStatisticObject
                                , zmqConnection, socket);
                        writeStatisticObject.setCurrentTimeEnd(System.currentTimeMillis());
                        writeStatisticObject.setRequestNumber(++writeRequests);
                        writeStatisticObject.setClientId(clientObject.getClientId());
                        writeStatisticObject.setRequestId("clid-" + clientObject.getClientId() + "-write-" + writeRequests + "-wlid" +
                                "-" + workloadId + "-bnid-" + ++bn);
                        updateContainedPayloadTypeByMap(build,
                                writeStatisticObject);
                        iStatistics.add(writeStatisticObject);

                    }

                } else {

                    /*if (Configuration.ENABLE_RATE_LIMITER_FOR_WRITE_PAYLOADS) {
                        rateLimiter.acquire();
                    }*/

                    build = BatchList.newBuilder().addBatches(batch).build();

                    List<String> eventList = new ArrayList<>();
                    if (Configuration.UPDATE_MEASURE_TIME == UpdateMeasureTimeType.BY_BATCH) {
                        eventList = updateStartTimeByMap(build, clientObject);
                    }

                    //handleRequestDistribution(clientObject.getClientId());

                    WriteStatisticObject writeStatisticObject = new WriteStatisticObject();

                    writeStatisticObject.getAssocEventList().addAll(eventList);

                    writeStatisticObject.setCurrentTimeStart(System.currentTimeMillis());
                    write(clientObject, prepareWorkloadObject, build, writeStatisticObject,
                            zmqConnection, socket);
                    writeStatisticObject.setCurrentTimeEnd(System.currentTimeMillis());
                    writeStatisticObject.setRequestNumber(++writeRequests);
                    writeStatisticObject.setClientId(clientObject.getClientId());
                    writeStatisticObject.setRequestId("clid-" + clientObject.getClientId() + "-write-" + writeRequests + "-wlid" +
                            "-" + workloadId + "-bnid-" + ++bn);
                    updateContainedPayloadTypeByMap(build,
                            writeStatisticObject);
                    iStatistics.add(writeStatisticObject);
                }
            }
        } else {

            /*if (Configuration.ENABLE_RATE_LIMITER_FOR_WRITE_PAYLOADS) {
                rateLimiter.acquire();
            }*/

            List<String> eventList = new ArrayList<>();
            if (Configuration.UPDATE_MEASURE_TIME == UpdateMeasureTimeType.BY_SEND) {
                eventList = updateStartTimeByMap(batchListToSend,
                        clientObject);
            }

            //handleRequestDistribution(clientObject.getClientId());

            WriteStatisticObject writeStatisticObject = new WriteStatisticObject();

            writeStatisticObject.getAssocEventList().addAll(eventList);

            writeStatisticObject.setCurrentTimeStart(System.currentTimeMillis());
            write(clientObject, prepareWorkloadObject, batchListToSend, writeStatisticObject,
                    zmqConnection, socket);
            writeStatisticObject.setCurrentTimeEnd(System.currentTimeMillis());
            writeStatisticObject.setRequestNumber(++writeRequests);
            writeStatisticObject.setClientId(clientObject.getClientId());
            writeStatisticObject.setRequestId("clid-" + clientObject.getClientId() + "-write-" + writeRequests +
                    "-wlid" +
                    "-" + workloadId + "-bnid-" + ++bn);
            updateContainedPayloadTypeByMap(batchListToSend,
                    writeStatisticObject);
            iStatistics.add(writeStatisticObject);
        }
        if (socket != null && socket.getLastEndpoint() != null) {
            boolean disconnect = socket.disconnect(socket.getLastEndpoint());
            socket.close();
            LOG.debug("Socket disconnected: " + disconnect);
        }
    }

    @Suspendable
    private List<String> updateStartTimeByMap(final BatchList batchList,
                                              final ClientObject clientObject) {
        List<String> eventList = new ArrayList<>();

        batchList.getBatchesList().forEach(batch -> batch.getTransactionsList().forEach(transaction -> {
            ISawtoothWritePayload payload = this.transactionDispatcher.getPayloadMapping().get(transaction);
            if (Configuration.DECODE_DATA_AS_CBOR_UPDATE_START_TIME) {
                try (ByteArrayInputStream byteArrayInputStream =
                             new ByteArrayInputStream(transaction.getPayload().toByteArray())) {
                    DataItem dataItem = new CborDecoder(byteArrayInputStream).decodeNext();
                    if (dataItem != null) {

                        Map<String, MutablePair<Long, Long>> valueMap =
                                ZmqListener.getObtainedEventsMap().get(clientObject.getClientId());

                        valueMap.get(payload.getEventPrefix() + payload.getSignature()).setLeft(System.nanoTime());

                        Map<String, MutablePair<Long, Long>> stringMutablePairMap =
                                ListenerReferenceValues.getTimeMap().computeIfAbsent(clientObject.getClientId(),
                                        c -> new ConcurrentHashMap<>());
                        stringMutablePairMap.computeIfAbsent(payload.getEventPrefix() + payload.getSignature(), m ->
                                MutablePair.of(System.currentTimeMillis(), -1L));

                        eventList.add(payload.getEventPrefix() + payload.getSignature());

                        LOG.debug("Updated time of payload before sending process: " + payload.getEventPrefix() + payload.getSignature());
                    } else {
                        LOG.error("Payload data item was null, not updating");
                    }
                } catch (CborException | IOException ex) {
                    LOG.error("Deserialization error, ignoring " + ex.getMessage());
                }
            } else {
                throw new NotYetImplementedException("Not yet implemented");
            }
        }));

        return eventList;
    }

    @Suspendable
    private List<String> updateStartTime(final List<ISawtoothWritePayload> iSawtoothWritePayloads,
                                         final BatchList batchList,
                                         final ClientObject clientObject) {
        List<ISawtoothWritePayload> payloadsToRemove = new ArrayList<>();
        List<String> eventList = new ArrayList<>();

        for (final Batch batch : batchList.getBatchesList()) {
            for (final Transaction transaction : batch.getTransactionsList()) {
                if (Configuration.DECODE_DATA_AS_CBOR_UPDATE_START_TIME) {
                    try (ByteArrayInputStream byteArrayInputStream =
                                 new ByteArrayInputStream(transaction.getPayload().toByteArray())) {
                        DataItem dataItem = new CborDecoder(byteArrayInputStream).decodeNext();
                        if (dataItem != null) {
                            for (final ISawtoothWritePayload payload : iSawtoothWritePayloads) {
                                if (dataItem.toString().contains(payload.getSignature())) {

                                    Map<String, MutablePair<Long, Long>> valueMap =
                                            ZmqListener.getObtainedEventsMap().get(clientObject.getClientId());

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
                        } else {
                            LOG.error("Payload data item was null, not updating");
                        }
                    } catch (CborException | IOException ex) {
                        LOG.error("Deserialization error, ignoring " + ex.getMessage());
                    }
                } else {
                    throw new NotYetImplementedException("Not yet implemented");
                }
            }
        }

                    /*batch -> batch.getTransactionsList().forEach(transaction -> iSawtoothWritePayloadsCcme.forEach
                    (payload -> {
                                if (Configuration.DECODE_DATA_AS_CBOR_UPDATE_START_TIME) {
                                    try (ByteArrayInputStream byteArrayInputStream =
                                                 new ByteArrayInputStream(transaction.getPayload().toByteArray())) {
                                        DataItem dataItem = new CborDecoder(byteArrayInputStream).decodeNext();
                                        if (dataItem != null) {
                                            if (dataItem.toString().contains(payload.getSignature())) {
                                                Map<String, MutablePair<Long, Long>> valueMap =
                                                        ZmqListener.getObtainedEventsMap().get(clientObject
                                                        .getClientId());

                                                valueMap.get(payload.getEventPrefix() + payload.getSignature())
                                                .setLeft(System.nanoTime());

                                                Map<String, MutablePair<Long, Long>> stringMutablePairMap =
                                                        ListenerReferenceValues.getTimeMap().computeIfAbsent
                                                        (clientObject.getClientId(),
                                                                c -> new ConcurrentHashMap<>());
                                                stringMutablePairMap.computeIfAbsent(payload.getEventPrefix() +
                                                payload.getSignature(), m ->
                                                        MutablePair.of(System.currentTimeMillis(), -1L));

                                                eventList.add(payload.getEventPrefix() + payload.getSignature());

                                                payloadsToRemove.add(payload);
                                                LOG.debug("Updated time of payload before sending process: " +
                                                payload.getEventPrefix() + payload.getSignature());
                                            }
                                            byteArrayInputStream.close();
                                        } else {
                                            LOG.error("Payload data item was null, not updating");
                                        }
                                    } catch (CborException | IOException ex) {
                                        LOG.error("Deserialization error, ignoring " + ex.getMessage());
                                    }
                                } else {
                                    throw new NotYetImplementedException("Not yet implemented");
                                }
                            })
                    )
        );*/
        iSawtoothWritePayloads.removeIf(payloadsToRemove::contains);
        /*Iterator<ISawtoothWritePayload> iterator = iSawtoothWritePayloads.iterator();
        while (iterator.hasNext()) {
            if (payloadsToRemove.contains(iterator.next())) {
                iterator.remove();
            }
        }*/
        //iSawtoothWritePayloads.removeAll(payloadsToRemove);
        return eventList;
    }

    @Suspendable
    private void updateContainedPayloadTypeByMap(final BatchList batchList,
                                                 final WriteStatisticObject writeStatisticObject) {

        batchList.getBatchesList().forEach(batch -> batch.getTransactionsList().forEach(transaction -> {
            ISawtoothWritePayload payload = this.transactionDispatcher.getPayloadMapping().get(transaction);
            if (Configuration.DECODE_DATA_AS_CBOR_UPDATE_START_TIME) {
                try (ByteArrayInputStream byteArrayInputStream =
                             new ByteArrayInputStream(transaction.getPayload().toByteArray())) {
                    DataItem dataItem = new CborDecoder(byteArrayInputStream).decodeNext();
                    if (dataItem != null) {
                        writeStatisticObject.getSpecificPayloadTypeList().add(payload.getSpecificPayloadType());
                    } else {
                        LOG.error("Payload data item was null, not updating");
                    }
                } catch (CborException | IOException ex) {
                    LOG.error("Deserialization error, ignoring " + ex.getMessage());
                }
            } else {
                throw new NotYetImplementedException("Not yet implemented");
            }
        }));

    }

    @Suspendable
    private void updateContainedPayloadType(final List<ISawtoothWritePayload> iSawtoothWritePayloads,
                                            final BatchList batchList,
                                            final WriteStatisticObject writeStatisticObject) {
        List<ISawtoothWritePayload> payloadsToRemove = new ArrayList<>();

        for (final Batch batch : batchList.getBatchesList()) {
            for (final Transaction transaction : batch.getTransactionsList()) {
                if (Configuration.DECODE_DATA_AS_CBOR_UPDATE_START_TIME) {
                    try (ByteArrayInputStream byteArrayInputStream =
                                 new ByteArrayInputStream(transaction.getPayload().toByteArray())) {
                        DataItem dataItem = new CborDecoder(byteArrayInputStream).decodeNext();
                        if (dataItem != null) {
                            for (final ISawtoothWritePayload payload : iSawtoothWritePayloads) {
                                if (dataItem.toString().contains(payload.getSignature())) {
                                    writeStatisticObject.getSpecificPayloadTypeList().add(payload.getSpecificPayloadType());
                                    payloadsToRemove.add(payload);
                                }
                            }
                        } else {
                            LOG.error("Payload data item was null, not updating");
                        }
                    } catch (CborException | IOException ex) {
                        LOG.error("Deserialization error, ignoring " + ex.getMessage());
                    }
                } else {
                    throw new NotYetImplementedException("Not yet implemented");
                }
            }
        }

        /*batchList.getBatchesList().forEach(
                batch -> batch.getTransactionsList().forEach(transaction -> iSawtoothWritePayloads.forEach(payload -> {
                            if (Configuration.DECODE_DATA_AS_CBOR_UPDATE_START_TIME) {
                                try (ByteArrayInputStream byteArrayInputStream =
                                             new ByteArrayInputStream(transaction.getPayload().toByteArray())) {
                                    DataItem dataItem = new CborDecoder(byteArrayInputStream).decodeNext();
                                    if (dataItem != null) {
                                        if (dataItem.toString().contains(payload.getSignature())) {
                                            writeStatisticObject.getSpecificPayloadTypeList().add(payload
                                            .getSpecificPayloadType());
                                            payloadsToRemove.add(payload);
                                        }
                                        byteArrayInputStream.close();
                                    } else {
                                        LOG.error("Payload data item was null");
                                    }
                                } catch (CborException | IOException ex) {
                                    LOG.error("Deserialization error, ignoring " + ex.getMessage());
                                }
                            } else {
                                throw new NotYetImplementedException("Not yet implemented");
                            }
                        })
                )
        );*/

        iSawtoothWritePayloads.removeIf(payloadsToRemove::contains);
        //iSawtoothWritePayloads.removeAll(payloadsToRemove);

    }

    @Suspendable
    private void write(final ClientObject clientObject, final PrepareSawtoothWorkloadObject prepareWorkloadObject
            , final BatchList batchListToSend, final WriteStatisticObject writeStatisticObject,
                       final ZmqConnection zmqConnection, final
                       ZMQ.Socket socket) {

        boolean hasError;
        String hasMessage;
        int e = 0;
        boolean timeSet = false;
        int retries = Configuration.RESEND_TIMES_UPON_ERROR_WRITE;
        do {
            ImmutablePair<Boolean, String> write;

            if (Configuration.CONNECTION_TYPE_WRITE == ConnectionEnum.ZMQ) {

                String serverAddress =
                        prepareWorkloadObject.getServerAddressesWrite().get(0);
                        /*GenericSelectionStrategy.selectFixed(Arrays.asList(Configuration
                        .VALIDATORS_TO_SEND_TRANSACTIONS_TO_ZMQ),
                                Collections.singletonList(0), false).get(0);*/

                writeStatisticObject.getParticipatingServers().add(serverAddress);

                if (Configuration.ENABLE_RATE_LIMITER_FOR_WRITE_PAYLOADS) {
                    rateLimiter.acquire(Configuration.NUMBER_OF_TRANSACTIONS_PER_BATCH_PER_CLIENT);
                    if (GeneralConfiguration.NOTE_RATE_LIMITER_WRITE == client.statistics.WriteStatisticObject.NoteRateLimiter.YES) {
                        if(!timeSet) {
                            writeStatisticObject.setCurrentTimeStart(System.currentTimeMillis());
                            updateStartTimeByMap(batchListToSend, clientObject);
                            timeSet = true;
                        }
                    }
                }

                ZmqConnection zmqConnectionCopy = zmqConnection;
                ZMQ.Socket socketCopy = socket;
                if (Configuration.SOCKET_CREATION_ENUM == SocketCreationEnum.BY_ACTION) {
                    zmqConnectionCopy = new ZmqConnection();
                    socketCopy = zmqConnectionCopy.createZmqListener();
                }

                zmqConnectionCopy.connectToZmq(socketCopy, serverAddress);

                WriteZmq writeZmq = new WriteZmq(Configuration.COMMIT_BATCHES_THRESHOLD,
                        Configuration.BATCH_CHECK_REPOLL_INTERVAL);

                //CompletableFuture<CompletableFuture<Boolean>> completableFutureCompletableFuture =
                //        CompletableFuture.supplyAsync(writeZmq::getIsDone);

                /*write =
                        writeZmq.write(batchListToSend, clientObject.getClientId() + "-write",
                                socketCopy, writeStatisticObject);*/

                write = writeZmq.write(batchListToSend,
                        clientObject.getClientId() +
                                "-write",
                        socketCopy, writeStatisticObject);

                /*try {
                    completableFutureCompletableFuture.get(Configuration.TIMEOUT_TRANSACTION, Configuration
                    .TIMEOUT_UNIT_TRANSACTION)
                            .get(Configuration.TIMEOUT_TRANSACTION, Configuration.TIMEOUT_UNIT_TRANSACTION);
                } catch (InterruptedException | ExecutionException ex) {
                    write = new ImmutablePair<>(true, ex.getMessage());
                } catch (TimeoutException ex) {
                    write = new ImmutablePair<>(true, "TIMEOUT_EX");
                }*/

                if (Configuration.SOCKET_CREATION_ENUM == SocketCreationEnum.BY_ACTION) {

                    /**/
                    if (socket != null && socket.getLastEndpoint() != null) {
                        socket.disconnect(socket.getLastEndpoint());
                        socket.close();
                    }
                    /**/

                    if (socketCopy.getLastEndpoint() != null) {
                        boolean disconnect = socketCopy.disconnect(socketCopy.getLastEndpoint());
                        socketCopy.close();
                        LOG.debug("Socket copy disconnected: " + disconnect);
                    }
                }

            } else if (Configuration.CONNECTION_TYPE_WRITE == ConnectionEnum.WebSocket) {
                byte[] batchListBytes = batchListToSend.toByteArray();
                String serverAddress =
                        GenericSelectionStrategy.selectFixed(Arrays.asList(Configuration.VALIDATORS_TO_SEND_TRANSACTIONS_TO_WEBSOCKET),
                                Collections.singletonList(0), false).get(0);

                writeStatisticObject.getParticipatingServers().add(serverAddress);

                WriteWebsocket writeWebsocket;
                if (Configuration.USE_PREPARED_WRITE_WEBSOCKET) {
                    writeWebsocket = prepareWorkloadObject.getWriteWebsocket();
                } else {
                    writeWebsocket = new WriteWebsocket();
                }

                if (Configuration.ENABLE_RATE_LIMITER_FOR_WRITE_PAYLOADS) {
                    rateLimiter.acquire(Configuration.NUMBER_OF_TRANSACTIONS_PER_BATCH_PER_CLIENT);
                    if (GeneralConfiguration.NOTE_RATE_LIMITER_WRITE == client.statistics.WriteStatisticObject.NoteRateLimiter.YES) {
                        if(!timeSet) {
                            writeStatisticObject.setCurrentTimeStart(System.currentTimeMillis());
                            updateStartTimeByMap(batchListToSend, clientObject);
                            timeSet = true;
                        }
                    }
                }

                write =
                        writeWebsocket.write(batchListBytes,
                                serverAddress, Configuration.COMMIT_BATCHES_THRESHOLD,
                                Configuration.BATCH_CHECK_REPOLL_INTERVAL, writeStatisticObject);
            } else {
                throw new NotYetImplementedException("Not yet implemented");
            }

            hasError = write.getLeft();
            hasMessage = write.getRight();

            if (hasError) {
                LOG.error("Had error (write) resend " + e + " message " + hasMessage);
                e++;
            }

            if (hasError && hasMessage != null) {
                writeStatisticObject.getErrorMessages().add(hasMessage);
            }

            if (Configuration.DROP_ON_TIMEOUT && write.getRight().contains("TIMEOUT_EX") || Configuration.DROP_ON_ERROR_4 && write.getRight().contains("Errno 4")) {
                LOG.error("Dropping write request due to exception " + write.getRight());
                break;
            }

        } while (hasError && e < retries);
        LOG.info("Number of resends (write): " + e);

        if (hasError) {
            writeStatisticObject.setFailedRequest(true);
        }

    }

    @Suspendable
    private List<ISawtoothWritePayload> prepareWritePayloads(final ClientObject clientObject,
                                                             final List<PrepareSawtoothWorkloadObject> listOfWorkloadObjects) {
        List<ISawtoothWritePayload> iSawtoothWritePayloads;
        if (Configuration.PREPARE_WRITE_PAYLOADS) {
            String id = clientObject.getClientId() + WRITE_SUFFIX;
            List<List<ISawtoothWritePayload>> sawtoothWritePayloads =
                    listOfWorkloadObjects.get(0).getSawtoothWritePayloads();
            iSawtoothWritePayloads = GenericSelectionStrategy.selectRoundRobin(sawtoothWritePayloads, 1, false,
                    false,
                    id, 1, false).get(0);
        } else {

            ISawtoothPayloads iSawtoothWritePayloadPattern = null;
            try {
                iSawtoothWritePayloadPattern =
                        Configuration.WRITE_PAYLOAD_PATTERN.getDeclaredConstructor().newInstance();
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
                ExceptionHandler.logException(ex);
            }

            iSawtoothWritePayloads =
                    (List<ISawtoothWritePayload>) iSawtoothWritePayloadPattern.getPayloads(clientObject,
                            Configuration.NUMBER_OF_TRANSACTION_PAYLOADS_PER_CLIENT);
        }
        return iSawtoothWritePayloads;
    }

    @Suspendable
    private List<ISawtoothReadPayload> prepareReadPayloads(final ClientObject clientObject,
                                                           final List<PrepareSawtoothWorkloadObject> listOfWorkloadObjects) {
        List<ISawtoothReadPayload> iSawtoothReadPayloads;
        if (Configuration.PREPARE_READ_PAYLOADS) {
            String id = clientObject.getClientId() + READ_SUFFIX;
            List<List<ISawtoothReadPayload>> sawtoothReadPayloads =
                    listOfWorkloadObjects.get(0).getSawtoothReadPayloads();
            iSawtoothReadPayloads = GenericSelectionStrategy.selectRoundRobin(sawtoothReadPayloads, 1, false, false,
                    id, 1, false).get(0);
        } else {

            ISawtoothPayloads iSawtoothReadPayloadPattern = null;
            try {
                iSawtoothReadPayloadPattern =
                        Configuration.READ_PAYLOAD_PATTERN.getDeclaredConstructor().newInstance();
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
                ExceptionHandler.logException(ex);
            }

            iSawtoothReadPayloads =
                    (List<ISawtoothReadPayload>) iSawtoothReadPayloadPattern.getPayloads(clientObject);
        }
        return iSawtoothReadPayloads;
    }

    @Suspendable
    private BatchList dispatch(final List<ISawtoothWritePayload> iSawtoothWritePayloads, final Signer signer,
                               long numberOfBatchesToSend, long numberOfTransactionsPerBatch,
                               BatchList batchListToSend, int batchCounter, final ClientObject clientObject) {

        List<ISawtoothWritePayload> iSawtoothWritePayloadsTmp = new ArrayList<>(iSawtoothWritePayloads);

        if (Configuration.DISPATCH_TRANSACTIONS_TO_BATCHES) {

            try {
                this.transactionDispatcher =
                        Configuration.I_TRANSACTION_TO_BATCH_DISPATCHER.getDeclaredConstructor().newInstance();

                while ((iSawtoothWritePayloadsTmp.size() > 0 && numberOfBatchesToSend > 0)) {
                    LOG.info("Remaining payloads: " + iSawtoothWritePayloadsTmp.size());
                    LOG.info("Current number of batches to send: " + numberOfBatchesToSend + " Number of transactions" +
                            " per" +
                            " batch: " + numberOfTransactionsPerBatch);

                    batchListToSend =
                            (BatchList) transactionDispatcher.dispatchTransactions(iSawtoothWritePayloadsTmp,
                                    signer,
                                    Configuration.NUMBER_OF_TRANSACTIONS_PER_BATCH_PER_CLIENT,
                                    batchListToSend.getBatchesList(),
                                    clientObject);

                    numberOfBatchesToSend--;
                    numberOfTransactionsPerBatch /*-=*/ =
                            batchListToSend.getBatchesList().get(batchCounter).getTransactionsCount();
                    LOG.trace("Transaction count: " + batchListToSend.getBatchesList().get(batchCounter).getTransactionsCount());

                    iSawtoothWritePayloadsTmp.subList(0,
                            batchListToSend.getBatchesList().get(batchCounter).getTransactionsCount()).clear();

                    batchCounter++;

                    LOG.info("Current number of payloads: " + iSawtoothWritePayloadsTmp.size() + ", " + " Current" +
                            " " +
                            "batch counter: "
                            + batchCounter
                            + ", Current number of transactions in batch: " + batchListToSend.getBatchesList().get(batchCounter - 1).getTransactionsCount());
                }
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
                ExceptionHandler.logException(ex);
            }
        } else {
            List<Transaction> transactionList = new ArrayList<>();

            try {
                this.transactionDispatcher =
                        Configuration.I_TRANSACTION_TO_BATCH_DISPATCHER.getDeclaredConstructor().newInstance();

                for (final ISawtoothWritePayload iSawtoothWritePayload : iSawtoothWritePayloadsTmp) {

                    Transaction transaction = SawtoothTransactionUtils.addTransactionToList(signer,
                            iSawtoothWritePayload.getFamilyName(),
                            iSawtoothWritePayload.getFamilyVersion(),
                            transactionList, iSawtoothWritePayload);

                    transactionDispatcher.getPayloadMapping().put(
                            transaction,
                            iSawtoothWritePayload
                    );

                    ZmqListener.getObtainedEventsMap().get(clientObject.getClientId()).get(iSawtoothWritePayload.getEventPrefix() + iSawtoothWritePayload.getSignature()).setLeft(System.nanoTime());

                }

            } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
                ExceptionHandler.logException(ex);
            }
            List<Batch> batches = new ArrayList<>(batchListToSend.getBatchesList());
            batches.add(SawtoothBatchUtils.prepareBatch(transactionList, signer));
            batchListToSend = SawtoothBatchUtils.buildBatchList(batches);
        }
        return batchListToSend;
    }

    @Suspendable
    private void prepareExpectedEventMap(final ClientObject clientObject,
                                         final List<ISawtoothWritePayload> iSawtoothWritePayloads) {

        for (final ISawtoothWritePayload iSawtoothWritePayload : iSawtoothWritePayloads) {

            String expectedEvent = iSawtoothWritePayload.getEventPrefix() + iSawtoothWritePayload.getSignature();

            Map<String, MutablePair<Long, Long>> stringMutablePairMap =
                    ZmqListener.getObtainedEventsMap().computeIfAbsent(clientObject.getClientId(),
                            c -> new ConcurrentHashMap<>());
            stringMutablePairMap.computeIfAbsent(expectedEvent, m ->
                    MutablePair.of(System.nanoTime(), -1L));

        }
    }

    @Suspendable
    private void debugDispatcherValues(final List<ISawtoothWritePayload> iSawtoothWritePayloads) {
        if (iSawtoothWritePayloads.size() != (Configuration.NUMBER_OF_BATCHES_PER_CLIENT * Configuration
                .NUMBER_OF_TRANSACTIONS_PER_BATCH_PER_CLIENT)) {
            LOG.info("Number of payloads not equal to set batches and transactions, set dispatcher: " + Configuration.I_TRANSACTION_TO_BATCH_DISPATCHER.getName());
        }
        if (iSawtoothWritePayloads.size() > (Configuration.NUMBER_OF_BATCHES_PER_CLIENT * Configuration
                .NUMBER_OF_TRANSACTIONS_PER_BATCH_PER_CLIENT)) {
            LOG.info("More payloads than set batches and transactions to dispatch");
        }
    }

    @Suspendable
    private void addToExpectedEventMap(final ClientObject clientObject,
                                       final String signature,
                                       final String expectedEventPrefix) {

        String expectedEvent = expectedEventPrefix + signature;

        Map<String, MutablePair<Long, Long>> stringMutablePairMap =
                ZmqListener.getObtainedEventsMap().computeIfAbsent(clientObject.getClientId(),
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
    @Suspendable
    @Override
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
