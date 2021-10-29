package client.commoninterfaces;

import co.paralleluniverse.fibers.Suspendable;

public interface IListenerLogic {

    @Suspendable
    <E> void handleEvent(E... params);

    @Suspendable
    <E> boolean checkThreshold(E... params);

}
