package sawtooth.payload_patterns;

import co.paralleluniverse.fibers.Suspendable;
import sawtooth.payloads.ISawtoothPayload;

import java.util.List;

public interface ISawtoothPayloads {

    @Suspendable
    <E> List<? extends ISawtoothPayload> getPayloads(E... params);

}
