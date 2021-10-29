package graphene.payloads;

import client.commoninterfaces.IBlockchainPayload;

public interface IGraphenePayload extends IBlockchainPayload {

    <E> E getPayload(E... params);

    String getSpecificPayloadType();

    void setSpecificPayloadType(String type);

}
