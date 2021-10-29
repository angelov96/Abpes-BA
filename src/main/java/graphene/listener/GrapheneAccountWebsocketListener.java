package graphene.listener;

import co.paralleluniverse.fibers.Suspendable;
import com.neovisionaries.ws.client.*;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class GrapheneAccountWebsocketListener {

    private static final Logger LOG = Logger.getLogger(GrapheneAccountWebsocketListener.class);
    private static CompletableFuture<Boolean> COMPLETABLE_FUTURE = new CompletableFuture<>();
    private static List<String> expectedResults;

    @Suspendable
    public static List<String> getExpectedResult() {
        return expectedResults;
    }

    @Suspendable
    public static synchronized void setExpectedResult(final List<String> expectedResults) {
        GrapheneAccountWebsocketListener.expectedResults = expectedResults;
    }

    @Suspendable
    public static CompletableFuture<Boolean> getCompletableFuture() {
        return COMPLETABLE_FUTURE;
    }

    @Suspendable
    public static void resetCompletableFuture() {
        COMPLETABLE_FUTURE = new CompletableFuture<>();
    }

    @Suspendable
    public WebSocketListener webSocketListener() {
        return new WebSocketListener() {
            @Suspendable
            @Override
            public void onStateChanged(final WebSocket websocket, final WebSocketState newState) {
            }

            @Suspendable
            @Override
            public void onConnected(final WebSocket websocket, final Map<String, List<String>> headers) {
            }

            @Suspendable
            @Override
            public void onConnectError(final WebSocket websocket, final WebSocketException cause) {
            }

            @Suspendable
            @Override
            public void onDisconnected(final WebSocket websocket, final WebSocketFrame serverCloseFrame,
                                       final WebSocketFrame clientCloseFrame, final boolean closedByServer) {
            }

            @Suspendable
            @Override
            public void onFrame(final WebSocket websocket, final WebSocketFrame frame) {
                LOG.info("Received frame");
                if (frame.isTextFrame()) {
                    for (final String expectedResult : expectedResults) {
                        if (frame.getPayloadText().endsWith(expectedResult)) {
                            LOG.info("Completed account preparation step");
                            COMPLETABLE_FUTURE.complete(true);
                        }
                    }
                    LOG.info("Text frame: " + frame.getPayloadText());
                }
            }

            @Suspendable
            @Override
            public void onContinuationFrame(final WebSocket websocket, final WebSocketFrame frame) {
            }

            @Suspendable
            @Override
            public void onTextFrame(final WebSocket websocket, final WebSocketFrame frame) {
            }

            @Suspendable
            @Override
            public void onBinaryFrame(final WebSocket websocket, final WebSocketFrame frame) {
            }

            @Suspendable
            @Override
            public void onCloseFrame(final WebSocket websocket, final WebSocketFrame frame) {
            }

            @Suspendable
            @Override
            public void onPingFrame(final WebSocket websocket, final WebSocketFrame frame) {
            }

            @Suspendable
            @Override
            public void onPongFrame(final WebSocket websocket, final WebSocketFrame frame) {
            }

            @Suspendable
            @Override
            public void onTextMessage(final WebSocket websocket, final String text) {
            }

            @Suspendable
            @Override
            public void onTextMessage(final WebSocket websocket, final byte[] data) {
            }

            @Suspendable
            @Override
            public void onBinaryMessage(final WebSocket websocket, final byte[] binary) {
            }

            @Suspendable
            @Override
            public void onSendingFrame(final WebSocket websocket, final WebSocketFrame frame) {
            }

            @Suspendable
            @Override
            public void onFrameSent(final WebSocket websocket, final WebSocketFrame frame) {
            }

            @Suspendable
            @Override
            public void onFrameUnsent(final WebSocket websocket, final WebSocketFrame frame) {
            }

            @Suspendable
            @Override
            public void onThreadCreated(final WebSocket websocket, final ThreadType threadType, final Thread thread) {
            }

            @Suspendable
            @Override
            public void onThreadStarted(final WebSocket websocket, final ThreadType threadType, final Thread thread) {
            }

            @Suspendable
            @Override
            public void onThreadStopping(final WebSocket websocket, final ThreadType threadType, final Thread thread) {
            }

            @Suspendable
            @Override
            public void onError(final WebSocket websocket, final WebSocketException cause) {
            }

            @Suspendable
            @Override
            public void onFrameError(final WebSocket websocket, final WebSocketException cause,
                                     final WebSocketFrame frame) {
            }

            @Suspendable
            @Override
            public void onMessageError(final WebSocket websocket, final WebSocketException cause,
                                       final List<WebSocketFrame> frames) {
            }

            @Suspendable
            @Override
            public void onMessageDecompressionError(final WebSocket websocket, final WebSocketException cause,
                                                    final byte[] compressed) {
            }

            @Suspendable
            @Override
            public void onTextMessageError(final WebSocket websocket, final WebSocketException cause,
                                           final byte[] data) {
            }

            @Suspendable
            @Override
            public void onSendError(final WebSocket websocket, final WebSocketException cause,
                                    final WebSocketFrame frame) {
            }

            @Suspendable
            @Override
            public void onUnexpectedError(final WebSocket websocket, final WebSocketException cause) {
            }

            @Suspendable
            @Override
            public void handleCallbackError(final WebSocket websocket, final Throwable cause) {
            }

            @Suspendable
            @Override
            public void onSendingHandshake(final WebSocket websocket, final String requestLine,
                                           final List<String[]> headers) {
            }
        };
    }

}
