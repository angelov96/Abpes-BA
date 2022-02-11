package diem.listener;

import client.client.ClientObject;
import client.commoninterfaces.IListenerDisconnectionLogic;
import client.commoninterfaces.IListenerLogic;
import client.statistics.IStatistics;
import co.paralleluniverse.fibers.Suspendable;
import corda.statistics.ListenerStatisticObject;
import net.corda.core.messaging.FlowProgressHandle;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Listener implements IListenerDisconnectionLogic, IListenerLogic {
    private static final Logger LOG = Logger.getLogger(diem.listener.Listener.class);
    private static final AtomicBoolean FINAL_STATS_RETRIEVED = new AtomicBoolean(false);
    private static final AtomicBoolean STATS_RETRIEVED = new AtomicBoolean(false);
    private final CompletableFuture<Boolean> isSubscribed = new CompletableFuture<>();
    private final double threshold;
    private final CompletableFuture<Boolean> done = new CompletableFuture<>();
    private final CompletableFuture<Boolean> subDone = new CompletableFuture<>();
    private final int numberOfExpectedEvents;
    private final int totalNumberOfExpectedEventsPerClient;
    private int currentNumberOfEventsPerClient = 0;
    private final double totalThreshold;
    private final Queue<IStatistics> iStatistics;
    private final Queue<ClientObject> clientObjects;
    private final List<String> clientIds = new ArrayList<>();
    private static final AtomicInteger RECEIVED_COUNTER = new AtomicInteger(0);
    private final AtomicBoolean statsSet = new AtomicBoolean(false);

    private static final Map<String, Map<String, MutablePair<Long, Long>>> OBTAINED_EVENTS_MAP =
            new ConcurrentHashMap<>();
    private static final Map<String, Map<String, String>> OBTAINED_TRANSACTION_ID_MAP =
            new ConcurrentHashMap<>();


    public Listener() {
        this.threshold = 0;
        this.iStatistics = null;
        this.numberOfExpectedEvents = 0;
        this.totalNumberOfExpectedEventsPerClient = 0;
        this.totalThreshold = 0;
        this.clientObjects = null;
    }
    public Listener(final int numberOfExpectedEventsConstructor,
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
    public void setStatisticsAfterTimeout() {
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

    @Override
    public <E> void handleEvent(E... params) {
        String id = (String) params[0];
        String progress = (String) params[1];
        FlowProgressHandle<?> flowProgressHandle = (FlowProgressHandle<?>) params[2];
        handleEvent(id, progress, flowProgressHandle);
    }

    @Suspendable
    private void handleEventLogic(final String id,
                                  final String progress, final FlowProgressHandle<?> flowProgressHandle) {
        if (!statsSet.get()) {
            if (done.isDone()) {
                return;
            }


        }
    }

    @Override
    public <E> boolean checkThreshold(E... params) {
        double threshold = (Double) params[0];
        int currentNumberOfEvents = (Integer) params[1];
        int numberOfExpectedEvents = (Integer) params[2];
        boolean isTotal = (Boolean) params[3];
        return checkThreshold(threshold, currentNumberOfEvents, numberOfExpectedEvents, isTotal);
    }

    @Suspendable
    private boolean checkThreshold(final double threshold, final int currentNumberOfEvents,
                                   final int numberOfExpectedEvents, final boolean isTotal) {
        if (threshold <= ((double) currentNumberOfEvents / numberOfExpectedEvents)) {
            LOG.info("Reached threshold of " + threshold + " aborting with value: " + ((double) currentNumberOfEvents / numberOfExpectedEvents)
                    + " current number of events: " + (double) currentNumberOfEvents + " number of expected events " + numberOfExpectedEvents
                    + " is total: " + isTotal);
            return true;
        }
        return false;
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
