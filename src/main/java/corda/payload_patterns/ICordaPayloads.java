package corda.payload_patterns;

import co.paralleluniverse.fibers.Suspendable;
import corda.payloads.ICordaPayload;

import java.util.List;

public interface ICordaPayloads {

    @Suspendable
    <E> List<? extends ICordaPayload> getPayloads(E... params);

}
