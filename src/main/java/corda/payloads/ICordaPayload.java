package corda.payloads;

import client.commoninterfaces.IBlockchainPayload;

public interface ICordaPayload extends IBlockchainPayload {

    String getSpecificPayloadType();

    void setSpecificPayloadType(String type);

}
