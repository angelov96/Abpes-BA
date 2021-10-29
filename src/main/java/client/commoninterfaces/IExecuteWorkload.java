package client.commoninterfaces;

import client.statistics.IStatistics;
import co.paralleluniverse.fibers.Suspendable;

import java.util.Queue;

public interface IExecuteWorkload extends IWorkload {

    @Suspendable
    <E> E executeWorkload(E... params);

    @Suspendable
    <E> E endWorkload(E... params);

    @Suspendable
    <E> Queue<IStatistics> getStatistics(E... params);

}
