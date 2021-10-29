package client.commoninterfaces;

import client.statistics.IStatistics;
import co.paralleluniverse.fibers.Suspendable;

import java.util.Queue;

public interface IPrepareWorkload extends IWorkload {

    @Suspendable
    <E> E prepareWorkload(final E... params);

    @Suspendable
    <E> E endPrepareWorkload(final E... params);

    @Suspendable
    <E> Queue<IStatistics> getStatistics(E... params);

}
