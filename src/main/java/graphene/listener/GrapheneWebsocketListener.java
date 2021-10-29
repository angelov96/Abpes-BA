package graphene.listener;

import client.commoninterfaces.IListenerDisconnectionLogic;
import client.statistics.IStatistics;
import co.paralleluniverse.fibers.Suspendable;
import com.neovisionaries.ws.client.*;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

public class GrapheneWebsocketListener implements IListenerDisconnectionLogic {

    private static final Logger LOG = Logger.getLogger(GrapheneWebsocketListener.class);

    @Suspendable
    public WebSocketListener webSocketListener() {
        return new WebSocketListener() {
            @Suspendable
            @Override
            public void onStateChanged(final WebSocket websocket, final WebSocketState newState) {
                LOG.info("State changed, uri: " + websocket.getURI().toString() + " state: " + newState.toString() +
                        " host: " + websocket.getURI().getHost() + " port: " + websocket.getURI().getPort());
            }

            @Suspendable
            @Override
            public void onConnected(final WebSocket websocket, final Map<String, List<String>> headers) {
                LOG.info("Connected");
            }

            @Suspendable
            @Override
            public void onConnectError(final WebSocket websocket, final WebSocketException cause) {
                LOG.info("Connection error");
            }

            @Suspendable
            @Override
            public void onDisconnected(final WebSocket websocket, final WebSocketFrame serverCloseFrame,
                                       final WebSocketFrame clientCloseFrame, final boolean closedByServer) {
                LOG.info("Disconnected " + "By server: " + closedByServer);
            }

            @Suspendable
            @Override
            public void onFrame(final WebSocket websocket, final WebSocketFrame frame) {
                LOG.info("Received frame");
                if (frame.isTextFrame()) {
                    LOG.info("Text frame: " + frame.getPayloadText());
                }
            }

            @Suspendable
            @Override
            public void onContinuationFrame(final WebSocket websocket, final WebSocketFrame frame) {
                LOG.info("Continuation frame");
            }

            @Suspendable
            @Override
            public void onTextFrame(final WebSocket websocket, final WebSocketFrame frame) throws Exception {
                if (frame.isTextFrame()) {
                    /*for (final String errorMessage : Configuration.ERROR_MESSAGES) {
                        if (frame.getPayloadText().contains(errorMessage)) {
                            throw new Exception(frame.getPayloadText());
                        }
                    }*/
                    LOG.info("Received text frame: " + frame.getPayloadText());
                }
            }

            @Suspendable
            @Override
            public void onBinaryFrame(final WebSocket websocket, final WebSocketFrame frame) {
                LOG.info("Received binary frame");
            }

            @Suspendable
            @Override
            public void onCloseFrame(final WebSocket websocket, final WebSocketFrame frame) {
                LOG.info("Received close frame");
            }

            @Suspendable
            @Override
            public void onPingFrame(final WebSocket websocket, final WebSocketFrame frame) {
                LOG.info("Received ping frame");
            }

            @Suspendable
            @Override
            public void onPongFrame(final WebSocket websocket, final WebSocketFrame frame) {
                LOG.info("Received pong frame");
            }

            @Suspendable
            @Override
            public void onTextMessage(final WebSocket websocket, final String text) {
                LOG.info("Received text message: " + text);
            }

            @Suspendable
            @Override
            public void onTextMessage(final WebSocket websocket, final byte[] data) {
                LOG.info("Received text message as bytes");
            }

            @Suspendable
            @Override
            public void onBinaryMessage(final WebSocket websocket, final byte[] binary) {
                LOG.info("Received binary message");
            }

            @Suspendable
            @Override
            public void onSendingFrame(final WebSocket websocket, final WebSocketFrame frame) {
                if (frame.isTextFrame()) {
                    LOG.info("Received sending frame: " + frame.getPayloadText());
                }
            }

            @Suspendable
            @Override
            public void onFrameSent(final WebSocket websocket, final WebSocketFrame frame) {
                LOG.info("Sent frame" + (frame.isTextFrame() ? ": " + frame.getPayloadText() : ""));
            }

            @Suspendable
            @Override
            public void onFrameUnsent(final WebSocket websocket, final WebSocketFrame frame) {
                LOG.info("Frame unsent" + (frame.isTextFrame() ? ": " + frame.getPayloadText() : ""));
            }

            @Suspendable
            @Override
            public void onThreadCreated(final WebSocket websocket, final ThreadType threadType, final Thread thread) {
                LOG.info("Thread created " + thread.getName());
            }

            @Suspendable
            @Override
            public void onThreadStarted(final WebSocket websocket, final ThreadType threadType, final Thread thread) {
                LOG.info("Thread started " + thread.getName());
            }

            @Suspendable
            @Override
            public void onThreadStopping(final WebSocket websocket, final ThreadType threadType, final Thread thread) {
                LOG.info("Thread stopped " + thread.getName());
            }

            @Suspendable
            @Override
            public void onError(final WebSocket websocket, final WebSocketException cause) {
                LOG.error("Error: " + cause.getMessage());
            }

            @Suspendable
            @Override
            public void onFrameError(final WebSocket websocket, final WebSocketException cause,
                                     final WebSocketFrame frame) {
                LOG.error("Frame error: " + cause.getMessage() + (frame.isTextFrame() ? ": " + frame.getPayloadText()
                        : ""));
            }

            @Suspendable
            @Override
            public void onMessageError(final WebSocket websocket, final WebSocketException cause,
                                       final List<WebSocketFrame> frames) {
                LOG.info("Message error: " + cause.getMessage());
                frames.forEach(frame -> LOG.info(frame.isTextFrame() ? ": " + frame.getPayloadText() : ""));
            }

            @Suspendable
            @Override
            public void onMessageDecompressionError(final WebSocket websocket, final WebSocketException cause,
                                                    final byte[] compressed) {
                LOG.error("Message decompression error: " + cause.getMessage());
            }

            @Suspendable
            @Override
            public void onTextMessageError(final WebSocket websocket, final WebSocketException cause,
                                           final byte[] data) {
                LOG.info("Text message error: " + cause.getMessage());
            }

            @Suspendable
            @Override
            public void onSendError(final WebSocket websocket, final WebSocketException cause,
                                    final WebSocketFrame frame) {
                LOG.info("Send error: " + cause.getMessage() + (frame.isTextFrame() ? ": " + frame.getPayloadText()
                        : ""));
            }

            @Suspendable
            @Override
            public void onUnexpectedError(final WebSocket websocket, final WebSocketException cause) {
                LOG.info("Unexpected error" + cause.getMessage());
            }

            @Suspendable
            @Override
            public void handleCallbackError(final WebSocket websocket, final Throwable cause) {
                LOG.error("Handle callback error" + cause.getMessage());
            }

            @Suspendable
            @Override
            public void onSendingHandshake(final WebSocket websocket, final String requestLine,
                                           final List<String[]> headers) {
                LOG.info("Sending handshake");
            }
        };
    }

    @Suspendable
    @Override
    public CompletableFuture<Boolean> isDone() {
        CompletableFuture<Boolean> done = new CompletableFuture<>();
        done.complete(true);
        return done;
    }

    @SafeVarargs
    @Override
    @Suspendable
    public final synchronized <E> Queue<IStatistics> getStatistics(final E... params) {
        return new ConcurrentLinkedQueue<>();
    }

    @Override
    @Suspendable
    public void setStatisticsAfterTimeout() {
    }
}
