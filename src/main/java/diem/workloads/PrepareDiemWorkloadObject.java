package diem.workloads;

import client.commoninterfaces.IListenerDisconnectionLogic;
import client.commoninterfaces.IWorkloadObject;
import com.neovisionaries.ws.client.WebSocket;
import diem.payloads.IDiemReadPayload;
import diem.payloads.IDiemWritePayload;
import graphene.helper.Helper;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.log4j.Logger;
import org.bitcoinj.core.ECKey;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class PrepareDiemWorkloadObject implements IWorkloadObject {

    private static final Logger LOG = Logger.getLogger(PrepareDiemWorkloadObject.class);
    private final List<String> acctIds = new ArrayList<>();
    private final List<WebSocket> websocketList = new ArrayList<>();
    private final Queue<IListenerDisconnectionLogic> iListenerDisconnectionLogicList = new ConcurrentLinkedQueue<>();
    private List<List<IDiemWritePayload>> completeWritePayloadList;
    private List<List<IDiemReadPayload>> completeReadPayloadList;
    private String signature;
    private byte[] chainId;
    private ECKey sourcePrivate;
    private List<ImmutableTriple<String, String, String>> keyServerAndAccountList;

    @Override
    public Queue<IListenerDisconnectionLogic> getIListenerDisconnectionLogicList() {
        return iListenerDisconnectionLogicList;
    }
    public byte[] getChainId() {
        return chainId;
    }

    public void setChainId(final byte[] chainId) {
        this.chainId = chainId;
    }

    public List<List<IDiemWritePayload>> getDiemWritePayloads() {
        return completeWritePayloadList;
    }

    public void setGrapheneWritePayloads(final List<List<IDiemWritePayload>> iDiemWritePayloads) {
        this.completeWritePayloadList = iDiemWritePayloads;
    }

    public static List<String> getServers() {
        return SERVERS;
    }

    private static final List<String> SERVERS = new ArrayList<>(Helper.getIpMap().keySet());

    public List<List<IDiemReadPayload>> getDiemReadPayloads() {
        return completeReadPayloadList;
    }

    public void setDiemReadPayloads(final List<List<IDiemReadPayload>> iDiemReadPayloads) {
        this.completeReadPayloadList = iDiemReadPayloads;
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


    public List<ImmutableTriple<String, String, String>> getKeyServerAndAccountList() {
        return keyServerAndAccountList;
    }

    public void setKeyServerAndAccountList(final List<ImmutableTriple<String, String, String>> keyServerAndAccountList) {
        this.keyServerAndAccountList = keyServerAndAccountList;
    }
}
