package quorum.read;

import co.paralleluniverse.fibers.Suspendable;
import org.apache.commons.lang3.tuple.ImmutablePair;

public interface IReadingMethod {

    @Suspendable
    <E> ImmutablePair<Boolean, String> read(E... params);

}
