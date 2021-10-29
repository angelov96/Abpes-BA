package fabric.payloads;

import fabric.configuration.Configuration;
import fabric.connection.FabricClient;
import org.apache.log4j.Logger;
import org.hyperledger.fabric.sdk.TransactionProposalRequest;
import org.hyperledger.fabric.sdk.TransactionRequest;

public class GeneralWritePayload implements IFabricWritePayload {

    private static final Logger LOG = Logger.getLogger(GeneralWritePayload.class);
    private static final TransactionRequest.Type CHAIN_CODE_LANGUAGE = Configuration.CHAINCODE_LANGUAGE;
    private String chainCodeName;
    private String chainCodeFunction;
    private String[] chainCodeArguments;
    private String signature;
    private String valueToRead;
    private String prefix;

    public TransactionRequest.Type getChainCodeLanguage() {
        return CHAIN_CODE_LANGUAGE;
    }

    @SafeVarargs
    @Override
    public final <E> E getPayload(final E... params) {
        return (E) this;
    }

    @Override
    public String getSignature() {
        return signature;
    }

    @Override
    public void setSignature(final String signature) {
        this.signature = signature;
    }

    @Override
    public <E> E getValueToRead() {
        return (E) valueToRead;
    }

    @Override
    public <E> void setValueToRead(final E valueToRead) {
        this.valueToRead = (String) valueToRead;
    }

    @Override
    public String getChainCodeName() {
        return chainCodeName;
    }

    public void setChainCodeName(final String chainCodeName) {
        this.chainCodeName = chainCodeName;
    }

    @SafeVarargs
    @Override
    public final <E> void setValues(final E... params) {
        setChainCodeName(String.valueOf(params[0]));
        setChainCodeFunction(String.valueOf(params[1]));
        /*List<Object> paramList = new ArrayList<>(Arrays.asList(params).subList(2, params.length));
        String[] paramArray = Arrays.copyOf(paramList.toArray(), paramList.size(), String[].class);*/
        setChainCodeArguments((String[]) params[2]);
    }

    public String getChainCodeFunction() {
        return chainCodeFunction;
    }

    public void setChainCodeFunction(String chainCodeFunctions) {
        this.chainCodeFunction = chainCodeFunctions;
    }

    public String[] getChainCodeArguments() {
        return chainCodeArguments;
    }

    public void setChainCodeArguments(final String[] chainCodeArguments) {
        this.chainCodeArguments = chainCodeArguments;
    }

    @Override
    public TransactionProposalRequest getTransactionRequest(final FabricClient fabricClient) {
        TransactionProposalRequest request = fabricClient.getInstance().newTransactionProposalRequest();

        //request.setChaincodeID(ccid);
        request.setChaincodeName(chainCodeName);
        request.setChaincodeLanguage(Configuration.CHAINCODE_LANGUAGE);
        request.setFcn(chainCodeFunction);
        request.setArgs(chainCodeArguments);
        request.setProposalWaitTime(Configuration.PROPOSAL_WAIT_TIME);
        return request;
    }

    @Override
    public String getEventPrefix() {
        if (prefix == null) {
            LOG.debug("Prefix is null");
            return "";
        }
        return prefix;
    }

    @Override
    public void setEventPrefix(final String prefix) {
        this.prefix = prefix;
    }

    private String specificPayloadType;

    @Override
    public String getSpecificPayloadType() {
        return specificPayloadType;
    }

    @Override
    public void setSpecificPayloadType(final String specificPayloadType) {
        this.specificPayloadType = specificPayloadType;
    }
}
