package graphene.read;

import co.paralleluniverse.fibers.Suspendable;

public interface IReadingMethod {

    @Suspendable
    <E> E read(E... params);

}
