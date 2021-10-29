package quorum.workloads;

import client.commoninterfaces.IListenerDisconnectionLogic;
import client.commoninterfaces.IWorkloadObject;
import client.supplements.ExceptionHandler;
import co.paralleluniverse.fibers.Suspendable;
import io.reactivex.disposables.Disposable;
import org.apache.log4j.Logger;
import org.web3j.crypto.Credentials;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.websocket.WebSocketService;
import quorum.configuration.Configuration;
import quorum.helper.Helper;
import quorum.payloads.IQuorumReadPayload;
import quorum.payloads.IQuorumWritePayload;
import quorum.write.WriteHelper;

import java.io.File;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public class PrepareQuorumWorkloadObject implements IWorkloadObject {

    private static final Logger LOG = Logger.getLogger(PrepareQuorumWorkloadObject.class);
    private final Queue<Disposable> listener = new ConcurrentLinkedQueue<>();
    private final Queue<IListenerDisconnectionLogic> iListenerDisconnectionLogicList = new ConcurrentLinkedQueue<>();
    private WebSocketService webSocketServiceWrite;

    private WebSocketService webSocketServiceRead;
    private String fromAddress;
    private String toAddress;
    private String password;
    private File walletFile;
    private Web3j web3jWrite;
    private Web3j web3jRead;
    private BigInteger value;
    private List<List<IQuorumWritePayload>> completeWritePayloadList;
    private List<List<IQuorumReadPayload>> completeReadPayloadList;
    private Credentials credentials;
    private AtomicLong nonce;
    private String nodeAddress;

    public static List<String> getServers() {
        return SERVERS;
    }

    private static final List<String> SERVERS = new ArrayList<>(Helper.getIpMap().keySet());

    public String getNodeAddress() {
        return nodeAddress;
    }

    public void setNodeAddress(final String nodeAddress) {
        this.nodeAddress = nodeAddress;
    }

    public WebSocketService getWebSocketServiceWrite() {
        return webSocketServiceWrite;
    }

    public void setWebSocketServiceWrite(final WebSocketService webSocketServiceWrite) {
        this.webSocketServiceWrite = webSocketServiceWrite;
    }

    public WebSocketService getWebSocketServiceRead() {
        return webSocketServiceRead;
    }

    public void setWebSocketServiceRead(final WebSocketService webSocketServiceRead) {
        this.webSocketServiceRead = webSocketServiceRead;
    }

    public String getFromAddress() {
        return fromAddress;
    }

    public void setFromAddress(final String fromAddress) {
        this.fromAddress = fromAddress;
    }

    public String getToAddress() {
        return toAddress;
    }

    public void setToAddress(final String toAddress) {
        this.toAddress = toAddress;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(final String password) {
        this.password = password;
    }

    public File getWalletFile() {
        return walletFile;
    }

    public void setWalletFile(final File walletFile) {
        this.walletFile = walletFile;
    }

    public Web3j getWeb3jWrite() {
        return web3jWrite;
    }

    public void setWeb3jWrite(final Web3j web3jWrite) {
        this.web3jWrite = web3jWrite;
    }

    public Web3j getWeb3jRead() {
        return web3jRead;
    }

    public void setWeb3jRead(final Web3j web3jRead) {
        this.web3jRead = web3jRead;
    }

    public BigInteger getValue() {
        return value;
    }

    public void setValue(final BigInteger value) {
        this.value = value;
    }

    public Queue<Disposable> getListener() {
        return listener;
    }

    public List<List<IQuorumWritePayload>> getQuorumWritePayloads() {
        return completeWritePayloadList;
    }

    public void setQuorumWritePayloads(final List<List<IQuorumWritePayload>> iQuorumWritePayloads) {
        this.completeWritePayloadList = iQuorumWritePayloads;
    }

    public List<List<IQuorumReadPayload>> getQuorumReadPayloads() {
        return completeReadPayloadList;
    }

    public void setQuorumReadPayloads(final List<List<IQuorumReadPayload>> iQuorumReadPayloads) {
        this.completeReadPayloadList = iQuorumReadPayloads;
    }

    public Credentials getCredentials() {
        return credentials;
    }

    public void setCredentials(final Credentials credentials) {
        this.credentials = credentials;
    }

    public Queue<IListenerDisconnectionLogic> getIListenerDisconnectionLogicList() {
        return iListenerDisconnectionLogicList;
    }

    public AtomicLong getNonce() {
        return nonce;
    }

    @Suspendable
    public synchronized void setNonce() {

        try {
            nonce =
                    WriteHelper.getNonce(web3jWrite,
                            fromAddress);

            if (Configuration.DISTRIBUTED_NONCE_HANDLING) {

                try (java.sql.Connection connection = client.database.Connection.getConnection()) {
                    String query = "INSERT INTO quorum_nonce AS qn (address, nonce)" +
                            "VALUES (?, ?)" +
                            "ON CONFLICT (address) DO NOTHING " +
                            "RETURNING nonce";

                    PreparedStatement preparedStatement = connection.prepareStatement(query);
                    preparedStatement.setString(1, fromAddress);
                    preparedStatement.setLong(2, nonce.get());

                    ResultSet resultSet = preparedStatement.executeQuery();

                    int i = 0;
                    while (resultSet.next()) {
                        LOG.info("Current nonce: " + resultSet.getInt("nonce"));
                        i++;
                    }
                    if (i == 0) {
                        LOG.info("Result set was empty");
                    }

                    resultSet.close();
                    preparedStatement.close();
                } catch (SQLException ex) {
                    ExceptionHandler.logException(ex);
                }
            }
        } catch (Exception ex) {
            ExceptionHandler.logException(ex);
        }

    }

}
