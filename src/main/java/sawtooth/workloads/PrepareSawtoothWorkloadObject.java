package sawtooth.workloads;

import client.commoninterfaces.IListenerDisconnectionLogic;
import client.commoninterfaces.IWorkloadObject;
import org.apache.log4j.Logger;
import org.zeromq.ZMQ;
import sawtooth.payloads.ISawtoothReadPayload;
import sawtooth.payloads.ISawtoothWritePayload;
import sawtooth.read.ReadWebsocket;
import sawtooth.sdk.signing.Signer;
import sawtooth.write.WriteWebsocket;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class PrepareSawtoothWorkloadObject implements IWorkloadObject {

    private static final Logger LOG = Logger.getLogger(PrepareSawtoothWorkloadObject.class);
    private final Queue<IListenerDisconnectionLogic> iListenerDisconnectionLogicList =
            new ConcurrentLinkedQueue<>();
    private final Map<String, ZMQ.Socket> zmqSocketSubscriptionServerMap =
            Collections.synchronizedMap(new LinkedHashMap<>());
    private final Queue<String> webSocketSubscriptionServers = new ConcurrentLinkedQueue<>();
    private List<List<ISawtoothWritePayload>> completeWritePayloadList;
    private List<List<ISawtoothReadPayload>> completeReadPayloadList;
    private WriteWebsocket writeWebsocket;
    private ReadWebsocket readWebsocket;
    private ZMQ.Socket socket;
    private Signer signer;

    @Override
    public Queue<IListenerDisconnectionLogic> getIListenerDisconnectionLogicList() {
        return iListenerDisconnectionLogicList;
    }

    public List<String> getServerAddressesWrite() {
        return serverAddressesWrite;
    }

    public void setServerAddressesWrite(final List<String> serverAddressesWrite) {
        this.serverAddressesWrite = serverAddressesWrite;
    }

    public List<String> getServerAddressesRead() {
        return serverAddressesRead;
    }

    public void setServerAddressesRead(final List<String> serverAddressesRead) {
        this.serverAddressesRead = serverAddressesRead;
    }

    private List<String> serverAddressesWrite;
    private List<String> serverAddressesRead;

    public WriteWebsocket getWriteWebsocket() {
        return writeWebsocket;
    }

    public void setWriteWebsocket(final WriteWebsocket writeWebsocket) {
        this.writeWebsocket = writeWebsocket;
    }

    public ReadWebsocket getReadWebsocket() {
        return readWebsocket;
    }

    public void setReadWebsocket(final ReadWebsocket readWebsocket) {
        this.readWebsocket = readWebsocket;
    }

    public List<List<ISawtoothWritePayload>> getSawtoothWritePayloads() {
        return completeWritePayloadList;
    }

    public void setSawtoothWritePayloads(final List<List<ISawtoothWritePayload>> iSawtoothWritePayloads) {
        this.completeWritePayloadList = iSawtoothWritePayloads;
    }

    public List<List<ISawtoothReadPayload>> getSawtoothReadPayloads() {
        return completeReadPayloadList;
    }

    public void setSawtoothReadPayloads(final List<List<ISawtoothReadPayload>> iSawtoothReadPayloads) {
        this.completeReadPayloadList = iSawtoothReadPayloads;
    }

    public ZMQ.Socket getSocket() {
        return socket;
    }

    public void setSocket(final ZMQ.Socket socket) {
        this.socket = socket;
    }

    public Signer getSigner() {
        return signer;
    }

    public void setSigner(final Signer signer) {
        LOG.debug("Using public key: " + signer.getPublicKey().hex());
        this.signer = signer;
    }

    public Queue<String> getWebSocketSubscriptionServers() {
        return webSocketSubscriptionServers;
    }

    public Map<String, ZMQ.Socket> getZmqSocketSubscriptionServerMap() {
        return zmqSocketSubscriptionServerMap;
    }

}
