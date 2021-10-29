package graphene.payload_patterns;

import co.paralleluniverse.fibers.Suspendable;
import graphene.payloads.IGraphenePayload;

import java.util.List;

public interface IGraphenePayloads {

    @Suspendable
    <E> List<? extends IGraphenePayload> getPayloads(E... params);

}
