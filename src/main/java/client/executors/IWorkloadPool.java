package client.executors;

import co.paralleluniverse.fibers.Suspendable;

public interface IWorkloadPool {

    @Suspendable
    <E> void execute(final E... params);

}
