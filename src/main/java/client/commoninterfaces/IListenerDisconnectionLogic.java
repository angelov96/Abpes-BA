package client.commoninterfaces;

import client.statistics.IStatistics;
import co.paralleluniverse.fibers.Suspendable;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;

public interface IListenerDisconnectionLogic {

    @Suspendable
    CompletableFuture<?> isDone();

    @Suspendable
    <E> Queue<IStatistics> getStatistics(E... params);

    @Suspendable
    void setStatisticsAfterTimeout();
}
