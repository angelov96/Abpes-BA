package graphene.workloads;

import client.commoninterfaces.IListenerDisconnectionLogic;
import client.commoninterfaces.IWorkloadObject;
import com.neovisionaries.ws.client.WebSocket;
import graphene.helper.Helper;
import graphene.payloads.IGrapheneReadPayload;
import graphene.payloads.IGrapheneWritePayload;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.log4j.Logger;
import org.bitcoinj.core.ECKey;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class PrepareGrapheneWorkloadObject implements IWorkloadObject {

    private static final Logger LOG = Logger.getLogger(PrepareGrapheneWorkloadObject.class);
    private final List<String> acctIds = new ArrayList<>();
    private final List<WebSocket> websocketList = new ArrayList<>();
    private final Queue<IListenerDisconnectionLogic> iListenerDisconnectionLogicList = new ConcurrentLinkedQueue<>();
    private List<List<IGrapheneWritePayload>> completeWritePayloadList;
    private List<List<IGrapheneReadPayload>> completeReadPayloadList;
    private String signature;
    private byte[] chainId;
    private ECKey sourcePrivate;
    private List<ImmutableTriple<String, String, String>> keyServerAndAccountList;

    public byte[] getChainId() {
        return chainId;
    }

    public void setChainId(final byte[] chainId) {
        this.chainId = chainId;
    }

    public List<List<IGrapheneWritePayload>> getGrapheneWritePayloads() {
        return completeWritePayloadList;
    }

    public void setGrapheneWritePayloads(final List<List<IGrapheneWritePayload>> iGrapheneWritePayloads) {
        this.completeWritePayloadList = iGrapheneWritePayloads;
    }

    public static List<String> getServers() {
        return SERVERS;
    }

    private static final List<String> SERVERS = new ArrayList<>(Helper.getIpMap().keySet());

    public List<List<IGrapheneReadPayload>> getGrapheneReadPayloads() {
        return completeReadPayloadList;
    }

    public void setGrapheneReadPayloads(final List<List<IGrapheneReadPayload>> iGrapheneReadPayloads) {
        this.completeReadPayloadList = iGrapheneReadPayloads;
    }

    public String getSignature() {
        return signature;
    }

    public void setSignature(final String signature) {
        this.signature = signature;
    }

    public ECKey getSourcePrivate() {
        return sourcePrivate;
    }

    public void setSourcePrivate(final ECKey sourcePrivate) {
        this.sourcePrivate = sourcePrivate;
    }

    public List<String> getAcctIds() {
        return acctIds;
    }

    public List<WebSocket> getWebsocketList() {
        return websocketList;
    }

    public Queue<IListenerDisconnectionLogic> getIListenerDisconnectionLogicList() {
        return iListenerDisconnectionLogicList;
    }

    public List<ImmutableTriple<String, String, String>> getKeyServerAndAccountList() {
        return keyServerAndAccountList;
    }

    public void setKeyServerAndAccountList(final List<ImmutableTriple<String, String, String>> keyServerAndAccountList) {
        this.keyServerAndAccountList = keyServerAndAccountList;
    }
}
