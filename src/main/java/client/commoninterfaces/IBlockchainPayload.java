package client.commoninterfaces;

import co.paralleluniverse.fibers.Suspendable;

public interface IBlockchainPayload {

    @Suspendable
    <E> E getPayload(final E... params);

    @Suspendable
    <E> void setValues(final E... params);

    // todo add expected result to specific payload

    // todo add number of requests to payload

    enum Payload_Type {
        READ,
        WRITE
    }
}
