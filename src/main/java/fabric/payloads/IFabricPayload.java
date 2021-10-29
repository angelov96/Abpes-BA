package fabric.payloads;

import client.commoninterfaces.IBlockchainPayload;
import fabric.connection.FabricClient;
import org.hyperledger.fabric.sdk.TransactionProposalRequest;

public interface IFabricPayload extends IBlockchainPayload {

    <E> E getPayload(E... params);

    <E> E getChainCodeFunction();

    <E> E getChainCodeArguments();

    <E> E getChainCodeLanguage();

    <E> E getChainCodeName();

    String getSpecificPayloadType();

    void setSpecificPayloadType(String type);

    TransactionProposalRequest getTransactionRequest(FabricClient fabricClient);

}
