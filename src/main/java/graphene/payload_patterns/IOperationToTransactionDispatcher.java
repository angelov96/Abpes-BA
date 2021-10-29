package graphene.payload_patterns;

import co.paralleluniverse.fibers.Suspendable;
import graphene.payloads.IGrapheneWritePayload;

import java.util.Map;

public interface IOperationToTransactionDispatcher {

    @Suspendable
    <E1, E2> E2 dispatchOperations(E1... params);

    @Suspendable
    <E> Map<E, IGrapheneWritePayload> getPayloadMapping();

}
