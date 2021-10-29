package client.statistics;

import co.paralleluniverse.fibers.Suspendable;

public interface IStatistics {

    @Suspendable
    <E> void writeStatistics(E... params);

    @Suspendable
    <E> void printStatistics(E... params);

    StatisticType getStatisticType();

    enum StatisticType {
        Write,
        Read,
        Listener,
        General,
        Custom,
        Block
    }

}
