package quorum.payload_patterns;

import co.paralleluniverse.fibers.Suspendable;
import quorum.payloads.IQuorumPayload;

import java.util.List;

public interface IQuorumPayloads {

    @Suspendable
    <E> List<? extends IQuorumPayload> getPayloads(E... params);

}
