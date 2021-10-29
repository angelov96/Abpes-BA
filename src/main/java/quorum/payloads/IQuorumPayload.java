package quorum.payloads;

import client.commoninterfaces.IBlockchainPayload;

import java.util.List;

public interface IQuorumPayload extends IBlockchainPayload {

    <E> E getPayload(E... params);

    <E> E getPayloadAsString(E... params);

    <E> void setValues(E... params);

    List getInputList();

    List getOutputList();

    String getSpecificPayloadType();

    void setSpecificPayloadType(String type);

}
