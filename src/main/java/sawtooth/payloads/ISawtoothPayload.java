package sawtooth.payloads;

import client.commoninterfaces.IBlockchainPayload;

public interface ISawtoothPayload extends IBlockchainPayload {

    <E> E getPayload(E... params);

    String getSpecificPayloadType();

    void setSpecificPayloadType(String type);

}
