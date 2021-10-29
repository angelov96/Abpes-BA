package fabric.workloads;

import client.commoninterfaces.IListenerDisconnectionLogic;
import client.commoninterfaces.IWorkloadObject;
import client.supplements.ExceptionHandler;
import fabric.connection.FabricClient;
import fabric.helper.Utils;
import fabric.payloads.IFabricReadPayload;
import fabric.payloads.IFabricWritePayload;
import org.apache.log4j.Logger;
import org.hyperledger.fabric.sdk.Channel;
import org.hyperledger.fabric.sdk.Orderer;
import org.hyperledger.fabric.sdk.Peer;
import org.hyperledger.fabric.sdk.exception.InvalidArgumentException;
import org.hyperledger.fabric.sdk.exception.TransactionException;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class PrepareFabricWorkloadObject implements IWorkloadObject {

    private static final Logger LOG = Logger.getLogger(PrepareFabricWorkloadObject.class);
    private final Map<String, Boolean> listener = Collections.synchronizedMap(new LinkedHashMap<>());
    private final Queue<IListenerDisconnectionLogic> iListenerDisconnectionLogicList = new ConcurrentLinkedQueue<>();
    private Channel channel;
    private FabricClient fabricClient;
    private String signature;
    private List<List<IFabricWritePayload>> completeWritePayloadList;
    private List<List<IFabricReadPayload>> completeReadPayloadList;

    public List<Peer> getPeerList() {
        return peerList;
    }

    public void setPeerList(final List<Peer> peerList) {
        this.peerList = peerList;
    }

    public List<Orderer> getOrdererList() {
        return ordererList;
    }

    public void setOrdererList(final List<Orderer> ordererList) {
        this.ordererList = ordererList;
    }

    private List<Peer> peerList;
    private List<Orderer> ordererList;

    public Map<String, Boolean> getListener() {
        return listener;
    }

    public Channel getChannel() {
        return channel;
    }

    public FabricClient getFabricClient() {
        return fabricClient;
    }

    public void setFabricClient(final FabricClient fabricClient) {
        this.fabricClient = fabricClient;
    }

    public void prepareChannel(final String channelName) {
        channel = Utils.prepareChannel(fabricClient, channelName);
        try {
            channel.initialize();
        } catch (InvalidArgumentException | TransactionException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    public List<List<IFabricWritePayload>> getFabricWritePayloads() {
        return completeWritePayloadList;
    }

    public void setFabricWritePayloads(final List<List<IFabricWritePayload>> iFabricWritePayloads) {
        this.completeWritePayloadList = iFabricWritePayloads;
    }

    public List<List<IFabricReadPayload>> getFabricReadPayloads() {
        return completeReadPayloadList;
    }

    public void setFabricReadPayloads(final List<List<IFabricReadPayload>> iFabricReadPayloads) {
        this.completeReadPayloadList = iFabricReadPayloads;
    }

    public String getSignature() {
        return signature;
    }

    public void setSignature(final String signature) {
        this.signature = signature;
    }

    public Queue<IListenerDisconnectionLogic> getIListenerDisconnectionLogicList() {
        return iListenerDisconnectionLogicList;
    }
}
