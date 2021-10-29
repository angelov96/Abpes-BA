package client.executors;

import co.paralleluniverse.fibers.Suspendable;

public interface IMainExecutor {

    @Suspendable
    <E> void startExecutor(E... params);

}
