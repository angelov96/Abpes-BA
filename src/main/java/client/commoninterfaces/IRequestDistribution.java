package client.commoninterfaces;

import co.paralleluniverse.fibers.Suspendable;

public interface IRequestDistribution {

    @Suspendable
    <E> void handleRequestDistribution(final E... params);

}
