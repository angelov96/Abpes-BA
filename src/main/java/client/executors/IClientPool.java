package client.executors;

import co.paralleluniverse.fibers.Suspendable;

public interface IClientPool {

    @Suspendable
    <E> void startClient(E... params);

}
