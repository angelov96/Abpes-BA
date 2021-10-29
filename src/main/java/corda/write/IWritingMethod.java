package corda.write;

import co.paralleluniverse.fibers.Suspendable;
import org.apache.commons.lang3.tuple.ImmutablePair;

public interface IWritingMethod {

    @Suspendable
    <E> ImmutablePair<Boolean, String> write(E... params);

}
