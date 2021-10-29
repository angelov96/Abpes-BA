package graphene.payloads;

import org.apache.log4j.Logger;
import org.hibernate.cfg.NotYetImplementedException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class GeneralReadPayload implements IGrapheneReadPayload {

    private static final Logger LOG = Logger.getLogger(GeneralReadPayload.class);
    private static final String JSON_RPC_VERSION = "2.0";
    private String method;
    private String[] params;
    private String id;
    private ReadPayloadType readPayloadType;

    private String[] objectIdsNode;

    private String objectId;

    @SafeVarargs
    @Override
    public final <E> E getPayload(final E... params) {

        if (readPayloadType.equals(ReadPayloadType.JSON_RPC)) {
            return (E) getJsonRpcRequest();
        } else if (readPayloadType.equals(ReadPayloadType.NODE)) {
            return (E) getIdList();
        } else if (readPayloadType.equals(ReadPayloadType.WALLET)) {
            return (E) objectId;
        } else {
            throw new NotYetImplementedException("Not yet implemented");
        }
    }

    private String getJsonRpcRequest() {

        String[] adjustedParams = new String[params.length];
        for (int i = 0; i < params.length; i++) {
            adjustedParams[i] = "\"" + params[i] + "\"";
        }

        return "{\"jsonrpc\": \"" + JSON_RPC_VERSION + "\", \"method\": \"" + method + "\", \"params\": " + Arrays.toString(adjustedParams) + "," +
                " \"id\":" + id + "}";
    }

    public List<Serializable> getIdList() {
        List<Serializable> idContainer = new ArrayList<>();
        List<String> idList = new ArrayList<>();
        Collections.addAll(idList, objectIdsNode);
        idContainer.add((Serializable) idList);
        return idContainer;
    }

    @SafeVarargs
    @Override
    public final <E> void setValues(final E... params) {

        ReadPayloadType readPayloadType = (ReadPayloadType) params[0];
        this.readPayloadType = readPayloadType;

        if (readPayloadType.equals(ReadPayloadType.JSON_RPC)) {
            method = (String) params[1];
            this.params = (String[]) params[2];
            id = (String) params[3];
        } else if (readPayloadType.equals(ReadPayloadType.NODE)) {
            objectIdsNode = (String[]) params[1];
        } else if (readPayloadType.equals(ReadPayloadType.WALLET)) {
            objectId = (String) params[1];
        } else {
            throw new NotYetImplementedException("Not yet implemented");
        }
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
