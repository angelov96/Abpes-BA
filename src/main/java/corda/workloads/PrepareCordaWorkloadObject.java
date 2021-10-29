package corda.workloads;

import client.commoninterfaces.IListenerDisconnectionLogic;
import client.commoninterfaces.IWorkloadObject;
import corda.listener.Listen;
import corda.payloads.ICordaReadPayload;
import corda.payloads.ICordaWritePayload;
import net.corda.core.messaging.CordaRPCOps;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class PrepareCordaWorkloadObject implements IWorkloadObject {

    private static final Logger LOG = Logger.getLogger(PrepareCordaWorkloadObject.class);
    private CordaRPCOps proxy;
    private List<List<ICordaWritePayload>> completeWritePayloadList;
    private List<List<ICordaReadPayload>> completeReadPayloadList;
    private final Map<String, Listen> listener = Collections.synchronizedMap(new LinkedHashMap<>());
    private final Queue<IListenerDisconnectionLogic> iListenerDisconnectionLogicList = new ConcurrentLinkedQueue<>();

    public Map<String, Listen> getListener() {
        return listener;
    }

    public CordaRPCOps getProxy() {
        return proxy;
    }

    public void setProxy(final CordaRPCOps proxy) {
        this.proxy = proxy;
    }

    public List<List<ICordaWritePayload>> getCordaWritePayloads() {
        return completeWritePayloadList;
    }

    public void setCordaWritePayloads(final List<List<ICordaWritePayload>> iCordaWritePayloads) {
        this.completeWritePayloadList = iCordaWritePayloads;
    }

    public List<List<ICordaReadPayload>> getCordaReadPayloads() {
        return completeReadPayloadList;
    }

    public void setCordaReadPayloads(final List<List<ICordaReadPayload>> iCordaReadPayloads) {
        this.completeReadPayloadList = iCordaReadPayloads;
    }

    @Override
    public Queue<IListenerDisconnectionLogic> getIListenerDisconnectionLogicList() {
        return iListenerDisconnectionLogicList;
    }
}
