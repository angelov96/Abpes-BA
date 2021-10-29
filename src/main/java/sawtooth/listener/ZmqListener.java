package sawtooth.listener;

import client.client.ClientObject;
import client.commoninterfaces.IListenerDisconnectionLogic;
import client.commoninterfaces.IListenerLogic;
import client.statistics.IStatistics;
import client.statistics.ListenerReferenceValues;
import client.supplements.ExceptionHandler;
import co.nstant.in.cbor.CborDecoder;
import co.nstant.in.cbor.CborException;
import co.nstant.in.cbor.model.DataItem;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.fibers.Suspendable;
import co.paralleluniverse.strands.Strand;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.log4j.Logger;
import org.spongycastle.util.encoders.DecoderException;
import org.zeromq.ZMQ;
import sawtooth.configuration.Configuration;
import sawtooth.helper.SawtoothHelper;
import sawtooth.sdk.protobuf.*;
import sawtooth.statistics.BlockStatisticObject;
import sawtooth.statistics.ListenerStatisticObject;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ZmqListener implements IListenerDisconnectionLogic, IListenerLogic {

    private static final Map<String, Map<String, MutablePair<Long, Long>>> OBTAINED_EVENTS_MAP =
            new ConcurrentHashMap<>();
    private static final Logger LOG = Logger.getLogger(ZmqListener.class);
    private static final AtomicBoolean STATS_RETRIEVED = new AtomicBoolean(false);
    private static final AtomicInteger RECEIVED_COUNTER = new AtomicInteger(0);
    private static final int DEFAULT_INVALID_VALUE = -1;
    private static final long DEFAULT_EXISTING_VALUE = -3;
    private final CompletableFuture<Boolean> done = new CompletableFuture<>();
    private final CompletableFuture<Boolean> isSubscribed = new CompletableFuture<>();
    private final int numberOfExpectedEvents;
    private final int totalNumberOfExpectedEventsPerClient;
    private final double threshold;
    private final AtomicBoolean statsSet = new AtomicBoolean(false);
    private final Queue<IStatistics> iStatistics;
    private final double totalThreshold;
    private int currentNumberOfEventsPerClient = 0;

    public ZmqListener(final int numberOfExpectedEventsConstructor,
                       final int totalNumberOfExpectedEventsPerClientConstructor,
                       final double thresholdConstructor,
                       final double totalThresholdConstructor,
                       final Queue<IStatistics> iStatisticsConstructor) {
        this.numberOfExpectedEvents = numberOfExpectedEventsConstructor;
        this.totalNumberOfExpectedEventsPerClient = totalNumberOfExpectedEventsPerClientConstructor;
        this.threshold = thresholdConstructor;
        this.iStatistics = iStatisticsConstructor;
        this.totalThreshold = totalThresholdConstructor;
    }

    @Suspendable
    public static Map<String, Map<String, MutablePair<Long, Long>>> getObtainedEventsMap() {
        return OBTAINED_EVENTS_MAP;
    }

    @Suspendable
    public static synchronized void subscribeListener(final ZMQ.Socket socket, final String correlationId) {
        Message message = buildClientSubscriptionMessage(correlationId,
                Message.MessageType.CLIENT_EVENTS_SUBSCRIBE_REQUEST);
        socket.send(message.toByteArray());
    }

    @Suspendable
    public static Message buildClientSubscriptionMessage(final String correlationId,
                                                         final Message.MessageType subscriptionMessageType) {
        return Message.newBuilder().setCorrelationId(correlationId).setMessageType(subscriptionMessageType).setContent(Configuration.SUBSCRIPTIONS.toByteString()).build();
    }

    @Suspendable
    public static synchronized void unsubscribeListener(final ZMQ.Socket socket, final String address) {
        socket.disconnect(address);
        socket.close();
    }

    @Suspendable
    @Override
    public CompletableFuture<Boolean> isDone() {
        return done;
    }

    @SafeVarargs
    @Override
    @Suspendable
    public synchronized final <E> Queue<IStatistics> getStatistics(final E... params) {
        if (statsSet.get() && !STATS_RETRIEVED.get()) {
            STATS_RETRIEVED.set(true);
            return iStatistics;
        } else {
            return new ConcurrentLinkedQueue<>();
        }
    }

    @Override
    @Suspendable
    public synchronized void setStatisticsAfterTimeout() {
        if(!statsSet.get()) {
            ListenerStatisticObject listenerStatisticObject = new ListenerStatisticObject();
            listenerStatisticObject.setObtainedEventsMap(OBTAINED_EVENTS_MAP);
            listenerStatisticObject.setSetThreshold(-1);
            listenerStatisticObject.setExpectedThreshold(threshold);
            listenerStatisticObject.setSetTotalThreshold(totalThreshold);
            iStatistics.add(listenerStatisticObject);
            statsSet.set(true);
        }
    }

    @Suspendable
    public CompletableFuture<Boolean> getIsSubscribed() {
        return isSubscribed;
    }

    @Suspendable
    public void inspectMessageTypes(final ZMQ.Socket socket, final String correlationId) {
        if (Configuration.ENABLE_DEBUGGING) {
            for (final Message.MessageType messageType : Message.MessageType.values()) {
                try {
                    Message message =
                            Message.newBuilder().setCorrelationId(correlationId).setMessageType(messageType).setContent(Configuration.SUBSCRIPTIONS.toByteString()).build();
                    socket.send(message.toByteArray());
                    LOG.info("Message type: " + messageType);
                } catch (IllegalArgumentException ex) {
                    ExceptionHandler.logException(ex);
                }
            }
        }
    }

    @Suspendable
    public void receiveZmq(final ZMQ.Socket socket, final Queue<ClientObject> clientObjectQueue, final long sleepTime) {
        try {
            while (true) {
                byte[] recv = socket.recv(Configuration.ZMQ_SOCKET_FLAG_WRITE);
                if (recv != null) {
                    try {
                        Message from = Message.parseFrom(recv);
                        LOG.debug("Correlation ID: " + from.getCorrelationId() + " Content: " + " Message type: " + from.getMessageType() + " name: " + from.getMessageType().name());

                        switch (from.getMessageType()) {
                            case CLIENT_EVENTS:
                                EventList eventList = EventList.parseFrom(from.getContent());
                                for (final Event event : eventList.getEventsList()) {
                                    LOG.debug("Number of attributes: " + event.getAttributesCount());
                                    for (final Event.Attribute attribute : event.getAttributesList()) {
                                        LOG.debug("Event Key: " + attribute.getKey() + " Event Value: " + attribute.getValue());
                                    }
                                    LOG.debug("Event data: " + event.getData().toString());
                                    LOG.debug("Event type: " + event.getEventType());

                                    if(Configuration.ENABLE_BLOCK_STATISTICS && Configuration.USE_ZMQ_FOR_BLOCK_STATISTICS) {
                                        if ("sawtooth/block-commit".equals(event.getEventType())) {
                                            if ("block_id".equals(event.getAttributesList().get(0).getKey())) {

                                                ClientBlockGetByIdRequest clientBlockGetByIdRequest =
                                                        ClientBlockGetByIdRequest.newBuilder().setBlockId(event.getAttributesList().get(0).getValue()).build();

                                                Message message =
                                                        Message.newBuilder().setMessageType(Message.MessageType.CLIENT_BLOCK_GET_BY_ID_REQUEST).setContent(clientBlockGetByIdRequest.toByteString()).build();
                                                socket.send(message.toByteArray());

                                            }
                                        }
                                    }

                                    if (Configuration.DECODE_DATA_AS_CBOR_LISTENER) {
                                        try {

                                            ByteArrayInputStream byteArrayInputStream =
                                                    new ByteArrayInputStream(event.getData().toByteArray());
                                            List<DataItem> decode =
                                                    new CborDecoder(byteArrayInputStream).decode();
                                            byteArrayInputStream.close();
                                            //CborDecoder.decode(event.getData().toByteArray());
                                            for (final DataItem dataItem : decode) {
                                                if (Configuration.EVENT_DECODER_LIST.contains(event.getEventType())) {

                                                    String expectedValue =
                                                            event.getEventType() + " " + SawtoothHelper.decodeCbor(dataItem);

                                                    if (handleEvent(socket, clientObjectQueue
                                                            , from,
                                                            expectedValue))
                                                        return;
                                                }
                                            }
                                        } catch (CborException | DecoderException ex) {
                                            LOG.error("Not able to decode data: " + event.getEventType());
                                        }
                                    } else {
                                        LOG.error("Not decoding and not measuring, extend as needed");
                                    }
                                }
                                break;
                            case CLIENT_EVENTS_SUBSCRIBE_RESPONSE:
                                ClientEventsSubscribeResponse clientEventsSubscribeResponse =
                                        ClientEventsSubscribeResponse.parseFrom(from.getContent());
                                LOG.debug("Subscription response: " + clientEventsSubscribeResponse.getResponseMessage() +
                                        " name: " + clientEventsSubscribeResponse.getStatus().name() + " " +
                                        "status " +
                                        "value: " + clientEventsSubscribeResponse.getStatusValue());
                                isSubscribed.complete(true);
                                break;
                            case PING_REQUEST:
                                LOG.trace("Received ping request");
                                if (Configuration.SEND_PING_RESPONSES) {
                                    Message message =
                                            Message.newBuilder().setMessageType(Message.MessageType.PING_RESPONSE).setContent(NetworkAcknowledgement.newBuilder().setStatus(NetworkAcknowledgement.Status.OK).build().toByteString()).build();
                                    boolean successfullySent = socket.send(message.toByteArray());
                                    LOG.trace("Successfully sent ping response: " + successfullySent + " from" +
                                            " " + Strand.currentStrand().getName());
                                }

                                if (statsSet.get()) {
                                    Message message =
                                            buildClientSubscriptionMessage(from.getCorrelationId(),
                                                    Message.MessageType.CLIENT_EVENTS_UNSUBSCRIBE_REQUEST);
                                    socket.send(message.toByteArray());
                                    LOG.error("Resent unsubscribe request");
                                }

                                break;
                            case CLIENT_EVENTS_UNSUBSCRIBE_RESPONSE:
                                ClientEventsUnsubscribeResponse clientEventsUnsubscribeResponse =
                                        ClientEventsUnsubscribeResponse.parseFrom(from.getContent());
                                LOG.info("Unsubscription response name: " + clientEventsUnsubscribeResponse.getStatus().name());
                                done.complete(true);
                                return;
                            case CLIENT_BLOCK_GET_RESPONSE:
                                ClientBlockGetResponse clientBlockGetResponse = ClientBlockGetResponse.parseFrom(from.getContent());
                                int transactionCounter =
                                        clientBlockGetResponse.getBlock().getBatchesList().stream().mapToInt(Batch::getTransactionsCount).sum();

                                BlockStatisticObject blockStatisticObject = new BlockStatisticObject();
                                blockStatisticObject.setBlockId(clientBlockGetResponse.getBlock().getHeaderSignature());
                                blockStatisticObject.setReceivedTime(System.currentTimeMillis());
                                blockStatisticObject.setClientId(clientObjectQueue.stream().map(ClientObject::getClientId).collect(Collectors.toList()).toString());
                                blockStatisticObject.setNumberOfTransactions(transactionCounter);
                                blockStatisticObject.setNumberOfActions(transactionCounter);
                                BlockHeader blockHeader =
                                        BlockHeader.parseFrom(clientBlockGetResponse.getBlock().getHeader());
                                blockStatisticObject.setBlockNum(blockHeader.getBlockNum());
                                clientBlockGetResponse.getBlock().getBatchesList().forEach(batch -> batch.getTransactionsList().forEach(transaction -> blockStatisticObject.getTxIdList().add(transaction.getHeaderSignature())));
                                iStatistics.add(blockStatisticObject);
                                break;
                            default:
                                LOG.debug("Received unhandled message with type: " + from.getMessageType() +
                                        " with correlation id: " + from.getCorrelationId());
                                break;
                        }
                    } catch (IOException ex) {
                        ExceptionHandler.logException(ex);
                    }
                }
                Strand.sleep(sleepTime);
            }
        } catch (ArithmeticException | IndexOutOfBoundsException | InterruptedException | SuspendExecution ex) {
            ExceptionHandler.logException(ex);
        }
    }

    @Suspendable
    private boolean handleEvent(final ZMQ.Socket socket, final Queue<ClientObject> clientObjectQueue, final Message from,
                                final String expectedValue) {

        if (Configuration.HANDLE_EVENT_SYNCHRONIZED) {
            synchronized (OBTAINED_EVENTS_MAP) {
                return handleEventLogic(socket, clientObjectQueue, from, expectedValue);
            }
        } else {
            return handleEventLogic(socket, clientObjectQueue, from, expectedValue);
        }
    }

    @Suspendable
    public boolean handleEventLogic(final ZMQ.Socket socket, final Queue<ClientObject> clientObjectQueue,
                                    final Message from,
                                    final String expectedValue) {

        if (!statsSet.get()) {

            if (done.isDone()) {
                return true;
            }

            clientObjectQueue.parallelStream().forEach(clientObject -> {
                //for (final ClientObject clientObject : clientObjectQueue) {
                String id = clientObject.getClientId();
                if (OBTAINED_EVENTS_MAP.get(id) == null) {
                    LOG.debug("Unknown map entry: " + id);
                } else {
                    if (OBTAINED_EVENTS_MAP.get(id).get(expectedValue) != null) {
                        MutablePair<Long, Long> longLongMutablePair =
                                OBTAINED_EVENTS_MAP.get(id).get(expectedValue);
                        if (longLongMutablePair.getRight() != DEFAULT_INVALID_VALUE) {
                            LOG.error("Updating already existing value " + expectedValue + " - possible duplicate" +
                                    " event received");
                            if (Configuration.RETURN_ON_EVENT_DUPLICATE) {
                                LOG.error("Returned due to duplicated");
                                //return false;
                                return;
                                //continue;
                            }
                        }
                        longLongMutablePair.setRight(System.nanoTime());

                        ListenerReferenceValues.getTimeMap().get(id).get(expectedValue).setRight(System.currentTimeMillis());

                        OBTAINED_EVENTS_MAP.get(id)
                                .replace(
                                        expectedValue,
                                        longLongMutablePair);
                        LOG.debug(id + " received expected value: " + expectedValue);
                        ++currentNumberOfEventsPerClient;
                        int receivedCounter = RECEIVED_COUNTER.incrementAndGet();
                        if (checkThreshold(threshold, currentNumberOfEventsPerClient,
                                numberOfExpectedEvents, false)) {
                            if (!statsSet.get() && checkThreshold(totalThreshold, receivedCounter, totalNumberOfExpectedEventsPerClient
                                    , true)) {
                                synchronized (this) {
                                    if(!statsSet.get()) {
                                        ListenerStatisticObject listenerStatisticObject = new ListenerStatisticObject();
                                        listenerStatisticObject.setObtainedEventsMap(OBTAINED_EVENTS_MAP);
                                        listenerStatisticObject.setSetThreshold(((double) receivedCounter / numberOfExpectedEvents));
                                        listenerStatisticObject.setExpectedThreshold(threshold);
                                        listenerStatisticObject.setSetTotalThreshold(totalThreshold);
                                        iStatistics.add(listenerStatisticObject);
                                        statsSet.set(true);
                                    }
                                }
                            }
                            Message message =
                                    buildClientSubscriptionMessage(from.getCorrelationId(),
                                            Message.MessageType.CLIENT_EVENTS_UNSUBSCRIBE_REQUEST);
                            socket.send(message.toByteArray());

                        }
                    } else {
                        LOG.debug("Received event value: " + expectedValue + " not contained for key: " + id);
                    }

                    //for (final String event : Configuration.EVENT_EXISTS_SUFFIX_LIST) {
                    Configuration.EVENT_EXISTS_SUFFIX_LIST.parallelStream().forEach(event -> {
                        if (!expectedValue.endsWith(event)) {
                            LOG.debug("Not checking for existing event");
                        } else {
                            if (OBTAINED_EVENTS_MAP.get(id).get(expectedValue.replace(event, "")) != null) {
                                MutablePair<Long, Long> longLongMutablePair =
                                        OBTAINED_EVENTS_MAP.get(id).get(expectedValue.replace(event, ""));
                                if (longLongMutablePair.getRight() != DEFAULT_INVALID_VALUE) {
                                    LOG.error("Updating already existing value " + expectedValue.replace(event, "") +
                                            " - possible duplicate" +
                                            " event received (existing)");
                                    if (Configuration.RETURN_ON_EVENT_DUPLICATE) {
                                        LOG.error("Returned due to duplicated (existing)");
                                        //return false;
                                        return;
                                        //continue;
                                    }
                                }
                                longLongMutablePair.setRight(DEFAULT_EXISTING_VALUE);
                                OBTAINED_EVENTS_MAP.get(id)
                                        .replace(
                                                expectedValue.replace(event, ""),
                                                longLongMutablePair);
                                LOG.debug(id + " received expected value (existing): " + expectedValue.replace(
                                        event, ""));
                                ++currentNumberOfEventsPerClient;
                                int receivedCounter = RECEIVED_COUNTER.incrementAndGet();
                                if (checkThreshold(threshold, currentNumberOfEventsPerClient,
                                        numberOfExpectedEvents, false)) {
                                    if (!statsSet.get() && checkThreshold(totalThreshold, receivedCounter,
                                            totalNumberOfExpectedEventsPerClient
                                            , true)) {
                                        synchronized (this) {
                                            if(!statsSet.get()) {
                                                ListenerStatisticObject listenerStatisticObject = new ListenerStatisticObject();
                                                listenerStatisticObject.setObtainedEventsMap(OBTAINED_EVENTS_MAP);
                                                listenerStatisticObject.setSetThreshold(((double) receivedCounter / numberOfExpectedEvents));
                                                listenerStatisticObject.setExpectedThreshold(threshold);
                                                listenerStatisticObject.setSetTotalThreshold(totalThreshold);
                                                iStatistics.add(listenerStatisticObject);
                                                statsSet.set(true);
                                            }
                                        }
                                    }
                                    Message message =
                                            buildClientSubscriptionMessage(from.getCorrelationId(),
                                                    Message.MessageType.CLIENT_EVENTS_UNSUBSCRIBE_REQUEST);
                                    socket.send(message.toByteArray());

                                }
                            }
                        }
                    });
                }
            });
        }
        return false;
    }

    @Suspendable
    private boolean checkThreshold(final double threshold, final int currentNumberOfEvents,
                                   final int numberOfExpectedEvents, final boolean isTotal) {
        if (threshold <= ((double) currentNumberOfEvents / numberOfExpectedEvents)) {
            LOG.info("Reached threshold of " + threshold + " aborting with value: " + ((double) currentNumberOfEvents / numberOfExpectedEvents) + " current number of events: " + (double) currentNumberOfEvents + " number of expected events " + numberOfExpectedEvents
                    + " is total: " + isTotal);
            return true;
        }
        return false;
    }

    @SafeVarargs
    @Override
    @Suspendable
    public final <E> void handleEvent(final E... params) {
        final ZMQ.Socket socket = (ZMQ.Socket) params[0];
        final Queue<ClientObject> clientObjectList = (Queue<ClientObject>) params[1];
        final Message from = (Message) params[2];
        final String expectedValue = (String) params[3];
        handleEvent(socket, clientObjectList, from, expectedValue);
    }

    @SafeVarargs
    @Override
    @Suspendable
    public final <E> boolean checkThreshold(final E... params) {
        double threshold = (Double) params[0];
        int currentNumberOfEvents = (Integer) params[1];
        int numberOfExpectedEvents = (Integer) params[2];
        boolean isTotal = (Boolean) params[3];
        return checkThreshold(threshold, currentNumberOfEvents, numberOfExpectedEvents, isTotal);
    }
}
