package corda.listener;

import client.client.ClientObject;
import client.commoninterfaces.IListenerDisconnectionLogic;
import client.commoninterfaces.IListenerLogic;
import client.statistics.IStatistics;
import client.statistics.ListenerReferenceValues;
import client.supplements.ExceptionHandler;
import co.paralleluniverse.fibers.Suspendable;
import corda.configuration.Configuration;
import corda.statistics.ListenerStatisticObject;
import javafx.collections.FXCollections;
import javafx.collections.ListChangeListener;
import javafx.collections.ObservableList;
import net.corda.core.contracts.ContractState;
import net.corda.core.flows.StateMachineRunId;
import net.corda.core.messaging.CordaRPCOps;
import net.corda.core.messaging.DataFeed;
import net.corda.core.messaging.FlowProgressHandle;
import net.corda.core.node.services.Vault;
import net.corda.core.node.services.vault.PageSpecification;
import net.corda.core.node.services.vault.QueryCriteria;
import net.corda.core.node.services.vault.Sort;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.log4j.Logger;
import rx.schedulers.Schedulers;

import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Listen implements IListenerDisconnectionLogic, IListenerLogic {

    private static final Logger LOG = Logger.getLogger(Listen.class);
    private static final Map<String, Map<String, String>> OBTAINED_TRANSACTION_ID_MAP =
            new ConcurrentHashMap<>();

    private static final Map<String, Map<String, MutablePair<Long, Long>>> OBTAINED_EVENTS_MAP =
            new ConcurrentHashMap<>();

    private static final AtomicInteger RECEIVED_COUNTER = new AtomicInteger(0);
    private static final int DEFAULT_INVALID_VALUE = -1;
    private static final long DEFAULT_EXISTING_VALUE = -3;
    private final int numberOfExpectedEvents;
    private final CompletableFuture<Boolean> done = new CompletableFuture<>();
    private final AtomicBoolean statsSet = new AtomicBoolean(false);

    private static final AtomicBoolean STATS_RETRIEVED = new AtomicBoolean(false);

    private final double threshold;
    private final Queue<IStatistics> iStatistics;
    private final int totalNumberOfExpectedEventsPerClient;
    private final double totalThreshold;
    private final CompletableFuture<Boolean> isSubscribed = new CompletableFuture<>();
    private int currentNumberOfEventsPerClient = 0;
    private final Queue<ClientObject> clientObjects;
    private final List<String> clientIds = new ArrayList<>();


    public Listen() {
        this.threshold = 0;
        this.iStatistics = null;
        this.numberOfExpectedEvents = 0;
        this.totalNumberOfExpectedEventsPerClient = 0;
        this.totalThreshold = 0;
        this.clientObjects = null;
    }

    public Listen(final int numberOfExpectedEventsConstructor,
                  final int totalNumberOfExpectedEventsPerClientConstructor,
                  final double thresholdConstructor,
                  final double totalThresholdConstructor,
                  final Queue<IStatistics> iStatisticsConstructor, final Queue<ClientObject> clientObjectsConstructor) {
        this.numberOfExpectedEvents = numberOfExpectedEventsConstructor;
        this.threshold = thresholdConstructor;
        this.totalNumberOfExpectedEventsPerClient = totalNumberOfExpectedEventsPerClientConstructor;
        this.iStatistics = iStatisticsConstructor;
        this.totalThreshold = totalThresholdConstructor;
        this.clientObjects = clientObjectsConstructor;
        clientObjects.forEach(c -> clientIds.add(c.getClientId()));
    }

    @Suspendable
    public static Map<String, String> getObtainedTransactionIdMap(final String id) {
        return OBTAINED_TRANSACTION_ID_MAP.get(id);
    }

    @Suspendable
    public static Map<String, Map<String, MutablePair<Long, Long>>> getObtainedEventsMap() {
        return OBTAINED_EVENTS_MAP;
    }

    @Suspendable
    public void vaultTrackBy(final CordaRPCOps proxy, final Class<? extends ContractState> clazz,
                             final QueryCriteria queryCriteria,
                             final PageSpecification pageSpecification, final Sort sort) {
        DataFeed<? extends Vault.Page<? extends ContractState>, ? extends Vault.Update<? extends ContractState>> pageUpdateDataFeed =
                proxy.vaultTrackBy(queryCriteria, pageSpecification,
                        sort,
                        clazz);

        LOG.trace("PageUpdateDataFeed: " + pageUpdateDataFeed);
        pageUpdateDataFeed.getUpdates().subscribeOn(Schedulers.io()).subscribe(subscription -> {
                    LOG.info("Subscription: " + subscription.toString());
                    subscription.getProduced().forEach(produced -> LOG.info(produced.toString() + " Produced: " + produced.getState().getData()));
                    subscription.getConsumed().forEach(consumed -> LOG.info(consumed.toString() + " Consumed:" + consumed.getState().getData()));
                    LOG.info("Flow id: " + subscription.getFlowId());
                    LOG.info("Type: " + subscription.getType());
                    subscription.getReferences().forEach(ref -> LOG.info("Reference: " + ref.toString()));
                    LOG.info("Subscription: " + subscription);
                }
                , ExceptionHandler::logException);
    }

    @Suspendable
    public void stateMachineRecordedTransactionMappingFeed(final CordaRPCOps proxy, final String id) {
        proxy.stateMachineRecordedTransactionMappingFeed().getUpdates().subscribeOn(Schedulers.io()).subscribe(feed -> {
                    LOG.info("UUID: " + feed.getStateMachineRunId().getUuid() + " TxId: " + feed.getTransactionId());
                    if (Configuration.SET_TRANSACTION_ID) {
                        Map<String, String> stringMutablePairMap =
                                OBTAINED_TRANSACTION_ID_MAP.computeIfAbsent(id,
                                        c -> new ConcurrentHashMap<>());
                        stringMutablePairMap.putIfAbsent(feed.getStateMachineRunId().getUuid().toString(),
                                feed.getTransactionId().toString());
                    }
                }
                , ExceptionHandler::logException);
    }

    @Suspendable
    public void stateMachinesFeed(final CordaRPCOps proxy) {
        proxy.stateMachinesFeed().getUpdates().subscribeOn(Schedulers.io()).subscribe(feed ->
                LOG.info("UUID: " + feed.getId().getUuid()), ExceptionHandler::logException);
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
            Objects.requireNonNull(this.iStatistics).add(listenerStatisticObject);
            statsSet.set(true);
        }
    }

    private static final ObservableList<ListenObject> OBSERVABLE_LIST =
           FXCollections.synchronizedObservableList(FXCollections.observableList(new ArrayList<>()));

    @Suspendable
    public static ObservableList<ListenObject> getObservableList() {
        return OBSERVABLE_LIST;
    }

    @Suspendable
    public void prepareListenForEvents() {

        OBSERVABLE_LIST.addListener((ListChangeListener<ListenObject>) change -> {
            while (change.next()) {
                if (change.wasAdded()) {
                    change.getAddedSubList().forEach(t -> {
                            if (Objects.requireNonNull(clientIds).contains(t.getId())) {
                                track(t.getFlowProgressHandle(), t.getId());
                            } else {
                                LOG.warn(t.getFlowProgressHandle().getId().getUuid() + ": " + t.getId() + " not contained in: " + clientIds + " only one subscriber allowed");
                            }
                    });
                }
            }
        });

        isSubscribed.complete(true);

    }

    @Suspendable
    public void track(final FlowProgressHandle<?> flowProgressHandle, final String id) {
            flowProgressHandle.getProgress().subscribeOn(Schedulers.io()).subscribe(progress -> {
                        LOG.info("Progress: " + progress);
                        handleEvent(id, progress, flowProgressHandle);
                    }, ExceptionHandler::logException
            );
    }

    @Suspendable
    private void handleEvent(final String id, final String progress, FlowProgressHandle<?> flowProgressHandle) {

        if (Configuration.HANDLE_EVENT_SYNCHRONIZED) {
            synchronized (OBTAINED_EVENTS_MAP) {
                handleEventLogic(id, progress, flowProgressHandle);
            }
        } else {
            handleEventLogic(id, progress, flowProgressHandle);
        }
    }

    private static final Queue<UUID> COMPLETED_FLOWS = new ConcurrentLinkedQueue<>();

    @Suspendable
    private void handleEventLogic(final String id,
                                  final String progress, final FlowProgressHandle<?> flowProgressHandle) {

        if (!statsSet.get()) {

            if (done.isDone()) {
                return;
            }

            if (Configuration.WAIT_FOR_FLOW_DONE && COMPLETED_FLOWS.contains(flowProgressHandle.getId().getUuid()) && "Done".equals(progress)) {
                if (checkThreshold(threshold, currentNumberOfEventsPerClient, numberOfExpectedEvents,
                        false)) {
                    int receivedCounter = RECEIVED_COUNTER.get();
                    if (!statsSet.get() && checkThreshold(totalThreshold, receivedCounter, totalNumberOfExpectedEventsPerClient
                            , true)) {
                        synchronized (this) {
                            if(!statsSet.get()) {
                                ListenerStatisticObject listenerStatisticObject = new ListenerStatisticObject();
                                listenerStatisticObject.setObtainedEventsMap(OBTAINED_EVENTS_MAP);
                                listenerStatisticObject.setSetThreshold(((double) receivedCounter / numberOfExpectedEvents));
                                listenerStatisticObject.setExpectedThreshold(threshold);
                                listenerStatisticObject.setSetTotalThreshold(totalThreshold);
                                Objects.requireNonNull(this.iStatistics).add(listenerStatisticObject);
                                statsSet.set(true);
                            }
                        }
                    }
                    if (Configuration.CLOSE_FLOW_POINT == CloseFlowPoint.LISTEN ||
                            Configuration.CLOSE_FLOW_POINT == CloseFlowPoint.FIRST
                    ) {
                        try {
                            flowProgressHandle.close();
                        } catch (CancellationException ex) {
                            LOG.error("Cancellation error during flow closing process");
                        }
                    }
                    done.complete(true);
                    return;
                }
            }

            if (OBTAINED_EVENTS_MAP.get(id) == null) {
                LOG.debug("Unknown map entry: " + id);
            } else {
                if (OBTAINED_EVENTS_MAP.get(id).get(progress) != null) {
                    MutablePair<Long, Long> longLongMutablePair =
                            OBTAINED_EVENTS_MAP.get(id).get(progress);
                    if (longLongMutablePair.getRight() != DEFAULT_INVALID_VALUE) {
                        LOG.error("Updating already existing value " + progress + " - possible duplicate event " +
                                "received");
                        if (Configuration.RETURN_ON_EVENT_DUPLICATE) {
                            LOG.error("Returned due to duplicated");
                            return;
                        }
                    }
                    longLongMutablePair.setRight(System.nanoTime());
                    ListenerReferenceValues.getTimeMap().get(id).get(progress).setRight(System.currentTimeMillis());

                    OBTAINED_EVENTS_MAP.get(id)
                            .replace(
                                    progress,
                                    longLongMutablePair);

                    LOG.debug(id + " received expected value: " + progress);
                    ++currentNumberOfEventsPerClient;

                    int receivedCounter = RECEIVED_COUNTER.incrementAndGet();

                    if (Configuration.WAIT_FOR_FLOW_DONE) {
                        COMPLETED_FLOWS.add(flowProgressHandle.getId().getUuid());
                    } else {
                        if (checkThreshold(threshold, currentNumberOfEventsPerClient, numberOfExpectedEvents,
                                false)) {
                            if (!statsSet.get() && checkThreshold(totalThreshold, receivedCounter, totalNumberOfExpectedEventsPerClient
                                    , true)) {
                                synchronized (this) {
                                    if(!statsSet.get()) {
                                        ListenerStatisticObject listenerStatisticObject = new ListenerStatisticObject();
                                        listenerStatisticObject.setObtainedEventsMap(OBTAINED_EVENTS_MAP);
                                        listenerStatisticObject.setSetThreshold(((double) receivedCounter / numberOfExpectedEvents));
                                        listenerStatisticObject.setExpectedThreshold(threshold);
                                        listenerStatisticObject.setSetTotalThreshold(totalThreshold);
                                        Objects.requireNonNull(this.iStatistics).add(listenerStatisticObject);
                                        statsSet.set(true);
                                    }
                                }
                            }
                            done.complete(true);
                            return;
                        }
                    }

                } else {
                    LOG.debug("Received event value: " + progress + " not contained for key: " + id);
                }

                //for (final String event : Configuration.EVENT_EXISTS_SUFFIX_LIST) {
                Configuration.EVENT_EXISTS_SUFFIX_LIST.parallelStream().forEach(event -> {
                    if (!event.endsWith(progress)) {
                        LOG.debug("Not checking for existing event");
                    } else {
                        if (OBTAINED_EVENTS_MAP.get(id).get(progress.replace(event, "")) != null) {
                            MutablePair<Long, Long> longLongMutablePair =
                                    OBTAINED_EVENTS_MAP.get(id).get(progress.replace(event, ""));
                            if (longLongMutablePair.getRight() != DEFAULT_INVALID_VALUE) {
                                LOG.error("Updating already existing value " + progress.replace(event, "") + " - " +
                                        "possible duplicate event " +
                                        "received (existing)");
                                if (Configuration.RETURN_ON_EVENT_DUPLICATE) {
                                    LOG.error("Returned due to duplicated (existing)");
                                    return;
                                    //continue;
                                }
                            }
                            longLongMutablePair.setRight(DEFAULT_EXISTING_VALUE);
                            OBTAINED_EVENTS_MAP.get(id)
                                    .replace(
                                            progress.replace(event, ""),
                                            longLongMutablePair);

                            LOG.debug(id + " received expected value (existing): " + progress.replace(event, ""));
                            ++currentNumberOfEventsPerClient;

                            int receivedCounter = RECEIVED_COUNTER.incrementAndGet();
                            if (Configuration.WAIT_FOR_FLOW_DONE) {
                                COMPLETED_FLOWS.add(flowProgressHandle.getId().getUuid());
                            } else {
                                if (checkThreshold(threshold, currentNumberOfEventsPerClient, numberOfExpectedEvents,
                                        false)) {
                                    if (!statsSet.get() && checkThreshold(totalThreshold, receivedCounter, totalNumberOfExpectedEventsPerClient
                                            , true)) {
                                        synchronized (this) {
                                            if(!statsSet.get()) {
                                                ListenerStatisticObject listenerStatisticObject = new ListenerStatisticObject();
                                                listenerStatisticObject.setObtainedEventsMap(OBTAINED_EVENTS_MAP);
                                                listenerStatisticObject.setSetThreshold(((double) receivedCounter / numberOfExpectedEvents));
                                                listenerStatisticObject.setExpectedThreshold(threshold);
                                                listenerStatisticObject.setSetTotalThreshold(totalThreshold);
                                                Objects.requireNonNull(this.iStatistics).add(listenerStatisticObject);
                                                statsSet.set(true);
                                            }
                                        }
                                    }
                                    done.complete(true);
                                    return;
                                }
                            }
                        }
                    }
                });

            }
        }
    }

    @Suspendable
    private boolean checkThreshold(final double threshold, final int currentNumberOfEvents,
                                   final int numberOfExpectedEvents, final boolean isTotal) {
        if (threshold <= ((double) currentNumberOfEvents / numberOfExpectedEvents)) {
            LOG.info("Reached threshold of " + threshold + " aborting with value: " + ((double) currentNumberOfEvents / numberOfExpectedEvents) + " current number of events: " + (double) currentNumberOfEvents + " number of expected events " + numberOfExpectedEvents + " is total: " + isTotal);
            return true;
        }
        return false;
    }

    @Suspendable
    public void trackWithFlowId(final FlowProgressHandle<?> flowProgressHandle, final String id,
                                final StateMachineRunId runId) {

        flowProgressHandle.getProgress().subscribeOn(Schedulers.io()).subscribe(progress -> {
                    if (!flowProgressHandle.getId().equals(runId)) {
                        LOG.error("RunID not equal");
                    }
                    LOG.info("Progress: " + progress);
                    handleEvent(id, progress);
                }, ExceptionHandler::logException
        );
    }

    @Suspendable
    public void trackStepsTreeFeed(final FlowProgressHandle<?> flowProgressHandle) {
        LOG.info("Steps tree feed: " + Objects.requireNonNull(flowProgressHandle.getStepsTreeFeed()));
        Objects.requireNonNull(flowProgressHandle.getStepsTreeFeed()).getUpdates().subscribeOn(Schedulers.io()).subscribe(
                list -> list.forEach(pair -> LOG.info("Key: " + pair.getFirst() + " Value: " + pair.getSecond())),
                ExceptionHandler::logException);
    }

    @Suspendable
    public void trackStepsTreeIndexFeed(final FlowProgressHandle<?> flowProgressHandle) {
        Objects.requireNonNull(flowProgressHandle.getStepsTreeIndexFeed()).getUpdates().subscribeOn(Schedulers.io()).subscribe(index -> LOG.info(
                "Index: " + index), ExceptionHandler::logException);
    }

    @SafeVarargs
    @Override
    @Suspendable
    public final <E> void handleEvent(final E... params) {
        String id = (String) params[0];
        String progress = (String) params[1];
        FlowProgressHandle<?> flowProgressHandle = (FlowProgressHandle<?>) params[2];
        handleEvent(id, progress, flowProgressHandle);
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

    @Suspendable
    @Override
    public CompletableFuture<Boolean> isDone() {
        return done;
    }

    @Suspendable
    public CompletableFuture<Boolean> getIsSubscribed() {
        return isSubscribed;
    }

}
