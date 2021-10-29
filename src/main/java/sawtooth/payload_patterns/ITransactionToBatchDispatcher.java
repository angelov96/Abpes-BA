package sawtooth.payload_patterns;

import co.paralleluniverse.fibers.Suspendable;
import sawtooth.payloads.ISawtoothWritePayload;

import java.util.Map;

public interface ITransactionToBatchDispatcher {

    @Suspendable
    <E> E dispatchTransactions(E... params);

    @Suspendable
    <E> Map<E, ISawtoothWritePayload> getPayloadMapping();

}
