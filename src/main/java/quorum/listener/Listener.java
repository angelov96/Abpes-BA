package quorum.listener;

import client.client.ClientObject;
import client.commoninterfaces.IListenerDisconnectionLogic;
import client.statistics.IStatistics;
import client.supplements.ExceptionHandler;
import co.paralleluniverse.fibers.Suspendable;
import io.reactivex.disposables.Disposable;
import io.reactivex.schedulers.Schedulers;
import org.apache.log4j.Logger;
import org.hibernate.cfg.NotYetImplementedException;
import org.web3j.abi.TypeReference;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.methods.request.EthFilter;
import org.web3j.protocol.core.methods.response.EthBlock;
import quorum.configuration.Configuration;
import quorum.helper.Helper;
import sawtooth.statistics.BlockStatisticObject;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class Listener implements IListenerDisconnectionLogic {

    private static final Logger LOG = Logger.getLogger(Listener.class);
    private static final AtomicBoolean FINAL_STATS_RETRIEVED = new AtomicBoolean(false);
    private static final AtomicBoolean STATS_RETRIEVED = new AtomicBoolean(false);
    private final CompletableFuture<Boolean> isSubscribed = new CompletableFuture<>();
    private final double threshold;
    private final CompletableFuture<Boolean> done = new CompletableFuture<>();
    private final CompletableFuture<Boolean> subDone = new CompletableFuture<>();
    private final int numberOfExpectedEvents;
    private final int totalNumberOfExpectedEventsPerClient;
    private final double totalThreshold;
    private final Queue<IStatistics> iStatistics = new ConcurrentLinkedQueue<>();

    public Listener(final int numberOfExpectedEventsConstructor,
                    final int totalNumberOfExpectedEventsPerClientConstructor, final double thresholdConstructor,
                    final double totalThresholdConstructor) {
        this.numberOfExpectedEvents = numberOfExpectedEventsConstructor;
        this.threshold = thresholdConstructor;
        this.totalThreshold = totalThresholdConstructor;
        this.totalNumberOfExpectedEventsPerClient = totalNumberOfExpectedEventsPerClientConstructor;
    }

    @Suspendable
    public static synchronized void unregisterListener(final Disposable listenerDisposable) {
        listenerDisposable.dispose();
    }

    @Suspendable
    public CompletableFuture<Boolean> getIsSubscribed() {
        return isSubscribed;
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
        if (STATS_RETRIEVED.get() && !FINAL_STATS_RETRIEVED.get()) {
            FINAL_STATS_RETRIEVED.set(true);
            return iStatistics;
        } else {
            return new ConcurrentLinkedQueue<>();
        }
    }

    @Override
    @Suspendable
    public synchronized void setStatisticsAfterTimeout() {
        ListenerHelper listenerHelper = new ListenerHelper();
        listenerHelper.setStatisticsAfterTimeout(threshold, totalThreshold);
        this.iStatistics.addAll(listenerHelper.getStatistics());
        STATS_RETRIEVED.set(true);
    }

    @Suspendable
    public Disposable registerBlockListener(final Web3j web3j, final Queue<ClientObject> clientObjectList, final boolean listenForFullTransactionObjects) {
        return web3j.blockFlowable(listenForFullTransactionObjects).subscribeOn(Schedulers.io()).subscribe(block -> {
            if (Configuration.DEBUG_SENT_TRANSACTION) {
                Helper.debugSend(block, "Listener, blockListener");
            }

            BlockStatisticObject blockStatisticObject = new BlockStatisticObject();
            blockStatisticObject.setBlockId(block.getBlock().getHash());
            blockStatisticObject.setReceivedTime(System.currentTimeMillis());
            blockStatisticObject.setClientId(clientObjectList.stream().map(ClientObject::getClientId).collect(Collectors.toList()).toString());
            blockStatisticObject.setNumberOfTransactions(block.getBlock().getTransactions().size());
            blockStatisticObject.setNumberOfActions(block.getBlock().getTransactions().size());
            blockStatisticObject.setBlockNum(block.getBlock().getNumber().longValue());
            block.getBlock().getTransactions().forEach(transactionResult ->
                    blockStatisticObject.getTxIdList().add(((EthBlock.TransactionObject) transactionResult.get()).getHash()));
            iStatistics.add(blockStatisticObject);

            LOG.info("Received block from listener: " + block.getBlock().getHash());
        }, ExceptionHandler::logException);
    }

    @Suspendable
    public Disposable registerEventLogListener(final Web3j web3j, final EthFilter ethFilter,
                                               final Queue<ClientObject> clientObjectQueue,
                                               final Queue<IStatistics> iStatistics) {
        ListenerHelper listenerHelper = new ListenerHelper();
        Disposable subscribe = web3j.ethLogFlowable(ethFilter).subscribeOn(Schedulers.io()).subscribe(event -> {
                    LOG.trace("Event data: " + event.getData());
                    listenerHelper.decodeListenerEvent(event, typeReferenceSelection(), clientObjectQueue,
                            numberOfExpectedEvents, totalNumberOfExpectedEventsPerClient, subDone, threshold,
                            totalThreshold, iStatistics);

                    if (subDone.isDone() && listenerHelper.getStatsSet().get() && !STATS_RETRIEVED.get()) {
                        this.iStatistics.addAll(listenerHelper.getStatistics());
                        STATS_RETRIEVED.set(true);
                        done.complete(true);
                    }
                }, ExceptionHandler::logException
        );

        isSubscribed.complete(true);
        return subscribe;
    }

    @Suspendable
    private List<TypeReference<?>> typeReferenceSelection() {
        List<TypeReference<?>> eventTypeReferenceList;
        if (Configuration.DEFAULT_EVENT_TYPE_REFERENCE_LIST != null) {
            eventTypeReferenceList =
                    Configuration.DEFAULT_EVENT_TYPE_REFERENCE_LIST;
        } else {
            throw new NotYetImplementedException("Not yet implemented type reference selection");
        }
        return eventTypeReferenceList;
    }

    @Suspendable
    public Disposable registerTransactionListener(final Web3j web3j) {
        return web3j.transactionFlowable().subscribeOn(Schedulers.io()).subscribe(tx -> {
            LOG.info("Received transaction from listener: " + tx.getHash());
        }, ExceptionHandler::logException);
    }

    @Suspendable
    public Disposable registerPendingTransactionListener(final Web3j web3j) {
        return web3j.pendingTransactionFlowable().subscribeOn(Schedulers.io()).subscribe(tx -> {
            LOG.info("Received pending transaction from listener: " + tx.getHash());
        }, ExceptionHandler::logException);
    }

}
