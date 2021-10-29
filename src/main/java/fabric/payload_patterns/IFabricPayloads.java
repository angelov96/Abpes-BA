package fabric.payload_patterns;

import co.paralleluniverse.fibers.Suspendable;
import fabric.payloads.IFabricPayload;

import java.util.List;

public interface IFabricPayloads {

    @Suspendable
    <E> List<? extends IFabricPayload> getPayloads(E... params);

}
