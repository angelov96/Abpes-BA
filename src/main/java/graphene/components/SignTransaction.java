package graphene.components;

import co.paralleluniverse.fibers.Suspendable;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.neovisionaries.ws.client.WebSocket;
import com.neovisionaries.ws.client.WebSocketException;
import com.neovisionaries.ws.client.WebSocketFrame;
import cy.agorise.graphenej.RPC;
import cy.agorise.graphenej.api.BaseGrapheneHandler;
import cy.agorise.graphenej.interfaces.WitnessResponseListener;
import cy.agorise.graphenej.models.ApiCall;
import cy.agorise.graphenej.models.BaseResponse;
import cy.agorise.graphenej.models.DynamicGlobalProperties;
import graphene.configuration.ApiId;
import graphene.configuration.Configuration;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class SignTransaction extends BaseGrapheneHandler {

    private static final Logger LOG = Logger.getLogger(SignTransaction.class);

    private final Transaction transaction;
    private final WitnessResponseListener listener;
    private final CompletableFuture<ImmutablePair<Boolean, String>> responseFuture = new CompletableFuture<>();
    private final boolean oneTime;

    public SignTransaction(final Transaction transaction,
                           final WitnessResponseListener listener) {
        this(transaction, true, listener);
    }

    public SignTransaction(final Transaction transactionConstructor, final boolean oneTimeConstructor
            , final WitnessResponseListener listenerConstructor) {
        super(listenerConstructor);
        this.transaction = transactionConstructor;
        this.oneTime = oneTimeConstructor;
        this.listener = listenerConstructor;
    }

    @Suspendable
    public CompletableFuture<ImmutablePair<Boolean, String>> getResponseFuture() {
        return responseFuture;
    }

    @Suspendable
    @Override
    public void onConnected(final WebSocket websocket, final Map<String, List<String>> headers) {
        List<Serializable> transactions = new ArrayList<>();
        transactions.add(transaction);
        transactions.add(Configuration.INSTANT_BROADCAST_AFTER_SIGNING);
        int currentId = 1;
        ApiCall networkApiIdCall = new ApiCall(ApiId.NON_RESTRICTED_API.ordinal(), "sign_transaction", transactions,
                RPC.VERSION,
                currentId);
        websocket.sendText(networkApiIdCall.toJsonString());
    }

    @Suspendable
    @Override
    public void onTextFrame(final WebSocket websocket, final WebSocketFrame frame) throws Exception {
        if (frame.isTextFrame()) {
            for (final String errorMessage : Configuration.ERROR_MESSAGES) {
                if (frame.getPayloadText().contains(errorMessage)) {
                    responseFuture.complete(ImmutablePair.of(true, frame.getPayloadText()));
                    if (oneTime) {
                        websocket.disconnect();
                    }
                    return;
                }
            }
            LOG.info("Signature received: " + frame.getPayloadText());
        }
        String response = frame.getPayloadText();
        GsonBuilder builder = new GsonBuilder();
        builder.registerTypeAdapter(DynamicGlobalProperties.class,
                new DynamicGlobalProperties.DynamicGlobalPropertiesDeserializer());
        Gson gson = builder.create();
        BaseResponse baseResponse = gson.fromJson(response, BaseResponse.class);
        if (baseResponse.error != null) {
            listener.onError(baseResponse.error);
        } else {
            responseFuture.complete(ImmutablePair.of(false, frame.getPayloadText()));
        }
        if (oneTime) {
            websocket.disconnect();
        }
    }

    @Override
    @Suspendable
    public void onFrameSent(final WebSocket websocket, final WebSocketFrame frame) {
        if (frame.isTextFrame()) {
            LOG.info("Signature sent: " + frame.getPayloadText());
        }
    }

    @Override
    @Suspendable
    public void onError(final WebSocket websocket, final WebSocketException cause) {
        LOG.error("onError, cause: " + cause.getMessage());
        listener.onError(new BaseResponse.Error(cause.getMessage()));
        responseFuture.complete(ImmutablePair.of(true, cause.getMessage()));
        if (oneTime) {
            websocket.disconnect();
        }
    }

    @Suspendable
    @Override
    public void handleCallbackError(final WebSocket websocket, final Throwable cause) {
        LOG.error("handleCallbackError, cause: " + cause.getMessage() + ", error: " + cause.getClass());
        for (final StackTraceElement element : cause.getStackTrace()) {
            LOG.error(element.getFileName() + "#" + element.getClassName() + ":" + element.getLineNumber());
        }
        listener.onError(new BaseResponse.Error(cause.getMessage()));
        responseFuture.complete(ImmutablePair.of(true, cause.getMessage()));
        if (oneTime) {
            websocket.disconnect();
        }
    }
}