package quorum.connection;

import client.supplements.ExceptionHandler;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.fibers.Suspendable;
import co.paralleluniverse.strands.Strand;
import org.apache.log4j.Logger;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.websocket.WebSocketService;
import org.web3j.utils.Async;
import quorum.configuration.Configuration;

import java.net.ConnectException;

public class Websocket {

    private static final Logger LOG = Logger.getLogger(Websocket.class);
    private String address;

    @Suspendable
    public static void closeConnection(final WebSocketService webSocketService) {
        webSocketService.close();
    }

    @Suspendable
    public WebSocketService prepareWebsocket(final String address) {
        WebSocketService webSocketService;
        boolean hasError;
        int i = 0;
        do {
            webSocketService = new WebSocketService(address, Configuration.INCLUDE_RAW_RESPONSES);
            try {
                webSocketService.connect();
                this.address = address;
                hasError = false;
            } catch (ConnectException ex) {
                try {
                    Strand.sleep(Configuration.WEBSOCKET_RECONNECTION_SLEEP_TIME);
                } catch (SuspendExecution | InterruptedException exception) {
                    ExceptionHandler.logException(exception);
                } finally {
                    ExceptionHandler.logException(ex);
                    hasError = true;
                }
            } finally {
                i++;
            }
        } while (hasError && i < Configuration.MAX_CONNECTION_RETRIES);
        return webSocketService;
    }

    @Suspendable
    public Web3j prepareWeb3j(final WebSocketService webSocketService) {
        return Web3j.build(webSocketService, Configuration.DEFAULT_BLOCK_TIME, Async.defaultExecutorService());
    }

    public String getAddress() {
        return address;
    }
}
