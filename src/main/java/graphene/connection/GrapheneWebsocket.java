package graphene.connection;

import client.supplements.ExceptionHandler;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.fibers.Suspendable;
import co.paralleluniverse.strands.Strand;
import com.neovisionaries.ws.client.WebSocket;
import com.neovisionaries.ws.client.WebSocketException;
import com.neovisionaries.ws.client.WebSocketFactory;
import com.neovisionaries.ws.client.WebSocketListener;
import cy.agorise.graphenej.test.NaiveSSLContext;
import graphene.configuration.Configuration;
import org.apache.log4j.Logger;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

public class GrapheneWebsocket {

    private static final Logger LOG = Logger.getLogger(GrapheneWebsocket.class);

    @Suspendable
    public static WebSocket connectToServer(final WebSocket webSocket, final List<WebSocketListener> listenerList) {
        WebSocket webSocketCopy = webSocket;
        boolean hasError;
        int i = 0;
        do {

            for (final WebSocketListener listener : listenerList) {
                webSocketCopy.addListener(listener);
            }

            try {
                if (Configuration.USE_CUSTOM_WEBSOCKET_EXECUTOR) {
                    webSocketCopy.connect(Configuration.CUSTOM_WEBSOCKET_EXECUTOR);
                } else {
                    webSocketCopy.connect();
                }
                hasError = false;
            } catch (WebSocketException ex) {
                try {
                    Strand.sleep(Configuration.WEBSOCKET_RECONNECTION_SLEEP_TIME);
                } catch (SuspendExecution | InterruptedException exception) {
                    ExceptionHandler.logException(exception);
                } finally {
                    webSocketCopy = prepareWebsocket(prepareWebsocketFactory(), webSocket.getURI().toASCIIString());
                    ExceptionHandler.logException(ex);
                    LOG.error("Retrying websocket connection for: " + i + " times");
                    hasError = true;
                }
            } finally {
                i++;
            }
        } while (hasError && i < Configuration.MAX_CONNECTION_RETRIES);
        return webSocketCopy;
    }

    @Suspendable
    public static WebSocketFactory prepareWebsocketFactory() {
        SSLContext context = null;
        try {
            context = NaiveSSLContext.getInstance("TLS");
        } catch (NoSuchAlgorithmException ex) {
            ExceptionHandler.logException(ex);
        }
        WebSocketFactory webSocketFactory = new WebSocketFactory();
        webSocketFactory.setSSLContext(context);
        webSocketFactory.setConnectionTimeout(Configuration.DEFAULT_FACTORY_WEBSOCKET_TIMEOUT);
        LOG.trace("Current connection timeout: " + webSocketFactory.getConnectionTimeout()
                + " Configuration timeout: " + Configuration.WEBSOCKET_TIMEOUT);
        return webSocketFactory;
    }

    @Suspendable
    public static WebSocket prepareWebsocket(final WebSocketFactory webSocketFactory, final String address) {
        WebSocket webSocket = null;
        try {
            webSocket = webSocketFactory.createSocket(address, /*Configuration.WEBSOCKET_TIMEOUT*/ 10000);
        } catch (IOException ex) {
            ExceptionHandler.logException(ex);
        }
        return webSocket;
    }

    @Suspendable
    public void addListener(final WebSocket webSocket, final WebSocketListener webSocketListener) {
        webSocket.addListener(webSocketListener);
        LOG.info("Set listener");
    }

}
